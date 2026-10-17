package schema

import "context"

// UserFormatID is the user-format step's stable id.
const UserFormatID = "0004-user-format"

// userFormatStamp advances user_format from absent to 1 and writes nothing else.
//
// A directory written before users existed holds no row in the user space, and
// version 1 with no users is exactly those bytes. There is nothing to convert,
// so the step is instant, takes no backup, and exists only because absent reads
// as older: without a step that closes the gap, [Registry.Plan] would refuse
// every directory written before this binary, naming a format it has never
// held a byte of.
func userFormatStamp() Migration {
	return Migration{
		ID:           UserFormatID,
		Requires:     map[string]uint32{"user_format": 0},
		Advances:     map[string]uint32{"user_format": 1},
		RewritesData: false,
		Strategy:     StrategyInstant,
		Run:          func(context.Context, *Context) error { return nil },
	}
}
