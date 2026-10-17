package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/users"
)

// UserStore is the user service the routes below administer. Every method
// reads its tenant from the context, which the routes set from the path.
type UserStore interface {
	Create(ctx context.Context, name string) (users.User, error)
	Rename(ctx context.Context, uid id.ID, name string) (users.User, error)
	SetState(ctx context.Context, uid id.ID, st users.State) (users.User, error)
	Get(ctx context.Context, uid id.ID) (users.User, error)
	List(ctx context.Context) ([]users.User, error)
	IssueToken(ctx context.Context, uid id.ID, expiresAt *time.Time) (users.Issued, error)
	ListTokens(ctx context.Context, uid id.ID) ([]users.Token, error)
	RevokeToken(ctx context.Context, uid, tid id.ID) (users.Token, error)
}

// UserResponse is one user as an operator sees it.
type UserResponse struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	State     string    `json:"state"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// UsersResponse is a tenant's users, oldest first.
type UsersResponse struct {
	Tenant string         `json:"tenant"`
	Users  []UserResponse `json:"users"`
}

// TokenResponse is one token, without its secret or anything it could be
// recovered from: a listing has no field either could travel in.
type TokenResponse struct {
	ID        string     `json:"id"`
	UserID    string     `json:"user_id"`
	State     string     `json:"state"`
	CreatedAt time.Time  `json:"created_at"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
}

// TokensResponse is a user's tokens, oldest first, in every state.
type TokensResponse struct {
	Tokens []TokenResponse `json:"tokens"`
}

// IssuedTokenResponse is the one response that carries a token's secret.
type IssuedTokenResponse struct {
	Token TokenResponse `json:"token"`
	// Secret is the whole token to present as a bearer credential. It is shown
	// here and never again.
	Secret string `json:"secret"`
}

// CreateUserRequest names a new user.
type CreateUserRequest struct {
	Name string `json:"name"`
}

// PatchUserRequest renames a user, changes its state, or both. Absent leaves a
// field alone.
type PatchUserRequest struct {
	Name  *string `json:"name"`
	State *string `json:"state"`
}

// IssueTokenRequest issues a token, optionally with an RFC 3339 expiry.
type IssueTokenRequest struct {
	ExpiresAt *time.Time `json:"expires_at"`
}

// usersScope checks the credential and resolves the path's tenant, and returns
// a context scoped to that tenant.
//
// The tenant is in the path, as it is for the retention policies beside these
// routes: a user is a property of a tenant, and creating one in the wrong
// tenant because a header was left over from the last request is a mistake
// with no undo — a user is never deleted. pathTenant holds a narrowed
// credential to its authorised set and applies the edition's confinement.
func (d Deps) usersScope(w http.ResponseWriter, r *http.Request) (context.Context, bool) {
	if !d.crossTenant(w, r) {
		return nil, false
	}
	if d.Users == nil {
		WriteError(w, r, errs.E(errs.Invalid, "http.users", errors.New(
			"this process does not store users")))
		return nil, false
	}
	t, ok := d.pathTenant(w, r, "http.users")
	if !ok {
		return nil, false
	}
	return tenant.NewContext(r.Context(), t), true
}

func pathUser(w http.ResponseWriter, r *http.Request) (id.ID, bool) {
	uid, err := id.Parse(r.PathValue("user"))
	if err != nil {
		WriteError(w, r, errs.E(errs.Invalid, "http.users", fmt.Errorf("%q is not a user id", r.PathValue("user"))))
		return id.Zero, false
	}
	return uid, true
}

func (d Deps) listUsers(w http.ResponseWriter, r *http.Request) {
	ctx, ok := d.usersScope(w, r)
	if !ok {
		return
	}
	list, err := d.Users.List(ctx)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	t, _ := tenant.FromContext(ctx)
	out := UsersResponse{Tenant: string(t), Users: make([]UserResponse, 0, len(list))}
	for _, u := range list {
		out.Users = append(out.Users, toUserResponse(u))
	}
	writeJSON(w, r, http.StatusOK, out)
}

func (d Deps) createUser(w http.ResponseWriter, r *http.Request) {
	ctx, ok := d.usersScope(w, r)
	if !ok {
		return
	}
	var body CreateUserRequest
	if !decodeJSON(w, r, &body) {
		return
	}
	u, err := d.Users.Create(ctx, body.Name)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	writeJSON(w, r, http.StatusCreated, toUserResponse(u))
}

func (d Deps) getUser(w http.ResponseWriter, r *http.Request) {
	ctx, ok := d.usersScope(w, r)
	if !ok {
		return
	}
	uid, ok := pathUser(w, r)
	if !ok {
		return
	}
	u, err := d.Users.Get(ctx, uid)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	writeJSON(w, r, http.StatusOK, toUserResponse(u))
}

// patchUser renames a user and changes its state. The name goes first, so a
// request that renames and disables, and finds the name taken, changes nothing.
func (d Deps) patchUser(w http.ResponseWriter, r *http.Request) {
	ctx, ok := d.usersScope(w, r)
	if !ok {
		return
	}
	uid, ok := pathUser(w, r)
	if !ok {
		return
	}
	var body PatchUserRequest
	if !decodeJSON(w, r, &body) {
		return
	}
	if body.Name == nil && body.State == nil {
		WriteMalformed(w, r, "name a field to change: name or state")
		return
	}
	var st users.State
	if body.State != nil {
		var err error
		if st, err = users.ParseState(*body.State); err != nil {
			WriteError(w, r, err)
			return
		}
	}
	u, err := d.Users.Get(ctx, uid)
	if body.Name != nil && err == nil {
		u, err = d.Users.Rename(ctx, uid, *body.Name)
	}
	if body.State != nil && err == nil {
		u, err = d.Users.SetState(ctx, uid, st)
	}
	if err != nil {
		WriteError(w, r, err)
		return
	}
	writeJSON(w, r, http.StatusOK, toUserResponse(u))
}

// deleteUser refuses, by name. A user is never deleted, because audit events
// and evidence refer to its id; the route exists so that the refusal says what
// to do instead, rather than a 405 that says nothing.
func (d Deps) deleteUser(w http.ResponseWriter, r *http.Request) {
	if _, ok := d.usersScope(w, r); !ok {
		return
	}
	WriteError(w, r, errs.E(errs.Invalid, "http.deleteUser", errors.New(
		"a user is never deleted, because its history refers to it by id; "+
			`disable it instead with PATCH {"state":"disabled"}, which refuses all of its tokens`)))
}

func (d Deps) listTokens(w http.ResponseWriter, r *http.Request) {
	ctx, ok := d.usersScope(w, r)
	if !ok {
		return
	}
	uid, ok := pathUser(w, r)
	if !ok {
		return
	}
	list, err := d.Users.ListTokens(ctx, uid)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	out := TokensResponse{Tokens: make([]TokenResponse, 0, len(list))}
	now := d.Clock.Now()
	for _, tok := range list {
		out.Tokens = append(out.Tokens, toTokenResponse(tok, now))
	}
	writeJSON(w, r, http.StatusOK, out)
}

func (d Deps) issueToken(w http.ResponseWriter, r *http.Request) {
	ctx, ok := d.usersScope(w, r)
	if !ok {
		return
	}
	uid, ok := pathUser(w, r)
	if !ok {
		return
	}
	var body IssueTokenRequest
	if !decodeJSON(w, r, &body) {
		return
	}
	issued, err := d.Users.IssueToken(ctx, uid, body.ExpiresAt)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	writeJSON(w, r, http.StatusCreated, IssuedTokenResponse{
		Token:  toTokenResponse(issued.Token, d.Clock.Now()),
		Secret: issued.Secret,
	})
}

func (d Deps) revokeToken(w http.ResponseWriter, r *http.Request) {
	ctx, ok := d.usersScope(w, r)
	if !ok {
		return
	}
	uid, ok := pathUser(w, r)
	if !ok {
		return
	}
	tid, err := id.Parse(r.PathValue("token"))
	if err != nil {
		WriteError(w, r, errs.E(errs.Invalid, "http.revokeToken", fmt.Errorf("%q is not a token id", r.PathValue("token"))))
		return
	}
	tok, err := d.Users.RevokeToken(ctx, uid, tid)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	writeJSON(w, r, http.StatusOK, toTokenResponse(tok, d.Clock.Now()))
}

func toUserResponse(u users.User) UserResponse {
	return UserResponse{
		ID: u.ID.String(), Name: u.Name, State: u.State.String(),
		CreatedAt: u.CreatedAt, UpdatedAt: u.UpdatedAt,
	}
}

func toTokenResponse(t users.Token, now time.Time) TokenResponse {
	return TokenResponse{
		ID: t.ID.String(), UserID: t.User.String(), State: t.StateAt(now),
		CreatedAt: t.CreatedAt, ExpiresAt: t.ExpiresAt, RevokedAt: t.RevokedAt,
	}
}
