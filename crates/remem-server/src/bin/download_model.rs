/// Helper binary: loads the fastembed model so Docker can pre-bake it into the image.
/// Run during `docker build` with FASTEMBED_CACHE_PATH set to a known directory.
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};

fn main() {
    println!("Downloading embedding model (all-MiniLM-L6-v2)…");
    TextEmbedding::try_new(InitOptions {
        model_name: EmbeddingModel::AllMiniLML6V2,
        show_download_progress: true,
        ..InitOptions::default()
    })
    .expect("Failed to download embedding model");
    println!("Model downloaded successfully.");
}
