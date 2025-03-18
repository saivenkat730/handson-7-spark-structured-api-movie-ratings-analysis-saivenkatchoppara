import pandas as pd
import random

# Define movie-related data
movies = [
    ("Inception", "Sci-Fi"),
    ("Titanic", "Romance"),
    ("Avengers: Endgame", "Action"),
    ("The Godfather", "Crime"),
    ("Forrest Gump", "Drama"),
    ("The Dark Knight", "Action"),
    ("Interstellar", "Sci-Fi"),
    ("Parasite", "Thriller"),
    ("The Shawshank Redemption", "Drama"),
    ("The Matrix", "Sci-Fi"),
    ("Toy Story", "Animation"),
    ("Pulp Fiction", "Crime"),
    ("The Lion King", "Animation"),
    ("Spider-Man: No Way Home", "Action"),
    ("The Grand Budapest Hotel", "Comedy"),
]

# Define locations, platforms, and age groups
locations = ["US", "UK", "India", "Canada", "Germany", "France", "Australia"]
platforms = ["Netflix", "Amazon", "Disney+", "HBO Max", "Hulu", "Apple TV"]
age_groups = ["Teen", "Adult", "Senior"]
subscription_statuses = ["Active", "Canceled"]

# Generate sample dataset
data = []
for i in range(1, 101):  # 100+ records
    movie_id = random.randint(100, 999)
    movie_title, genre = random.choice(movies)
    rating = round(random.uniform(2.0, 5.0), 1)
    review_count = random.randint(1, 50)
    watched_year = random.randint(2018, 2023)
    user_location = random.choice(locations)
    age_group = random.choice(age_groups)
    streaming_platform = random.choice(platforms)
    watch_time = random.randint(60, 240)  # Watch time in minutes
    is_binge_watched = random.choice([True, False])
    subscription_status = random.choice(subscription_statuses)

    data.append([
        i, movie_id, movie_title, genre, rating, review_count, watched_year, 
        user_location, age_group, streaming_platform, watch_time, 
        is_binge_watched, subscription_status
    ])

# Create DataFrame
columns = [
    "UserID", "MovieID", "MovieTitle", "Genre", "Rating", "ReviewCount",
    "WatchedYear", "UserLocation", "AgeGroup", "StreamingPlatform", 
    "WatchTime", "IsBingeWatched", "SubscriptionStatus"
]
df = pd.DataFrame(data, columns=columns)

# Save dataset to CSV
df.to_csv("input/movie_ratings_data.csv", index=False)

print("Dataset generated successfully: input/movie_ratings_data.csv")
