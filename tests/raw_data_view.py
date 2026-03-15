from src.core.config import get_settings
import pandas as pd

settings = get_settings()

movies_path = settings.DATA_RAW_DIR / "normalized_movies.csv"
embeddings_path = settings.DATA_RAW_DIR / "movie_embeddings.csv"

movies = pd.read_csv(movies_path)
embeddings = pd.read_csv(embeddings_path)

movies['tmdbId'] = movies['tmdbId'].astype(str)
embeddings['tmdbId'] = embeddings['tmdbId'].astype(str)

print(embeddings['title'][0], embeddings['embedding'][0])

data = pd.merge(movies, embeddings, on="tmdbId", how="inner")

print(f"Movies shape {movies.shape}")
print(f"embeddings shape {embeddings.shape}")
print(f"merged data shape {data.shape}")

with pd.option_context(
    'display.max_rows', 10,
    'display.max_columns', None,
    'display.width', None,
    'display.max_colwidth', 150
):
    display_df = data.head(10).copy()
    display_df['embedding'] = display_df['embedding'].str[:80] + ' ... ' + display_df['embedding'].str[-40:]
    print(display_df)
