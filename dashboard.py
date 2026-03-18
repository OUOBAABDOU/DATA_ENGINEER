import streamlit as st
import pandas as pd
import os
import glob
import matplotlib.pyplot as plt

st.title("ETL Data Visualization Dashboard")

st.markdown("This dashboard visualizes the processed data from the ETL pipeline.")


def normalize_image_url(url):
    if pd.isna(url) or not str(url).strip():
        return ""
    normalized = str(url).strip()
    if normalized.startswith("http://"):
        normalized = "https://" + normalized[len("http://"):]
    return normalized


def resolve_image_source(row):
    local_image_path = row.get('local_image_path', '')
    if pd.notna(local_image_path) and str(local_image_path).strip() and os.path.exists(str(local_image_path)):
        return str(local_image_path)
    return row.get('image_url', '')


def get_image_status(row):
    local_image_path = row.get('local_image_path', '')
    image_url = row.get('image_url', '')
    if pd.notna(local_image_path) and str(local_image_path).strip() and os.path.exists(str(local_image_path)):
        return "Downloaded"
    if pd.notna(image_url) and str(image_url).strip():
        return "Download failed"
    return "No image"


def infer_theme(row):
    title = str(row.get('title', '')).lower()
    utility = str(row.get('utility', '')).lower()
    class_name = str(row.get('class', '')).strip()
    normalized_class = class_name.lower()

    if normalized_class and normalized_class not in {'one', 'two', 'three', 'four', 'five', 'book', 'general'}:
        return class_name

    theme_keywords = {
        "History": ["history", "war", "empire", "king", "queen", "normandy", "pirate"],
        "Science": ["science", "physics", "astronomy", "biology", "virus", "theory"],
        "Business": ["business", "money", "finance", "market", "sale", "brand"],
        "Travel": ["travel", "journey", "harbor", "trip", "pilgrimage", "woods"],
        "Health": ["health", "flu", "soul", "mind", "body", "medical"],
        "Cooking": ["kitchen", "cook", "recipe", "food"],
        "Religion & Philosophy": ["philosophy", "mythology", "buddhism", "love", "spiritual"],
        "Art & Creativity": ["art", "creative", "creativity", "picture", "music"],
        "Fiction": ["novel", "story", "fiction"],
    }

    combined_text = f"{title} {utility}"
    for theme, keywords in theme_keywords.items():
        if any(keyword in combined_text for keyword in keywords):
            return theme
    return "General"


def get_quality_score(value):
    rating_map = {
        "one": 1,
        "two": 2,
        "three": 3,
        "four": 4,
        "five": 5,
        "fiction": 3,
        "non-fiction": 3,
        "magazine": 2,
        "book": 3,
        "general": 2,
    }
    return rating_map.get(str(value).strip().lower(), 2)


def get_appreciation(score):
    if score >= 4.5:
        return "Excellent"
    if score >= 3.5:
        return "Good"
    if score >= 2.5:
        return "Average"
    return "Low"

# Load data from processed directory
processed_dir = os.path.join(os.path.dirname(__file__), 'storage', 'processed')

if os.path.exists(processed_dir):
    all_files = glob.glob(os.path.join(processed_dir, "**", "*.csv"), recursive=True)
    if all_files:
        dfs = []
        for file in all_files:
            df = pd.read_csv(file)
            df['source_file'] = os.path.basename(file)
            dfs.append(df)
        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            if 'image_url' in combined_df.columns:
                combined_df['image_url'] = combined_df['image_url'].apply(normalize_image_url)
            if 'local_image_path' not in combined_df.columns:
                combined_df['local_image_path'] = ''
            combined_df['display_image'] = combined_df.apply(resolve_image_source, axis=1)
            combined_df['image_status'] = combined_df.apply(get_image_status, axis=1)
            combined_df['theme'] = combined_df.apply(infer_theme, axis=1)
            combined_df['quality_score'] = combined_df['class'].apply(get_quality_score) if 'class' in combined_df.columns else 2
            for metric_column in ['read_count', 'order_count', 'bought_count']:
                if metric_column not in combined_df.columns:
                    combined_df[metric_column] = 0
                combined_df[metric_column] = pd.to_numeric(combined_df[metric_column], errors='coerce').fillna(0).astype(int)
            if 'appreciation' not in combined_df.columns:
                combined_df['appreciation'] = combined_df['quality_score'].apply(get_appreciation)
            else:
                combined_df['appreciation'] = combined_df['appreciation'].fillna(
                    combined_df['quality_score'].apply(get_appreciation)
                )

            if 'image_status' in combined_df.columns:
                st.subheader("Image Download Status")
                status_counts = combined_df['image_status'].value_counts()
                status_columns = st.columns(len(status_counts))
                for index, (status, count) in enumerate(status_counts.items()):
                    status_columns[index].metric(status, int(count))
                st.bar_chart(status_counts)

            st.subheader("Books Grouped by Theme")
            theme_summary = (
                combined_df.groupby('theme', dropna=False)
                .agg(
                    books_count=('title', 'count'),
                    images_count=('display_image', lambda values: int(values.fillna('').str.strip().ne('').sum())),
                    average_quality=('quality_score', 'mean'),
                    read_count=('read_count', 'sum'),
                    order_count=('order_count', 'sum'),
                    bought_count=('bought_count', 'sum'),
                )
                .reset_index()
            )
            theme_summary['average_quality'] = theme_summary['average_quality'].round(2)
            appreciation_summary = (
                combined_df.groupby('theme', dropna=False)['appreciation']
                .agg(lambda values: values.mode().iloc[0] if not values.mode().empty else 'Average')
                .reset_index()
            )
            theme_summary = theme_summary.merge(appreciation_summary, on='theme', how='left')
            st.dataframe(theme_summary)
            st.bar_chart(theme_summary.set_index('theme')[['books_count', 'images_count']])
            st.subheader("Theme Activity Metrics")
            st.bar_chart(theme_summary.set_index('theme')[['read_count', 'order_count', 'bought_count']])

            st.subheader("All Processed Data")
            st.dataframe(combined_df)

            # Summary statistics
            st.subheader("Summary Statistics")
            st.write(combined_df.describe())

            # Visualize by class
            if 'class' in combined_df.columns:
                st.subheader("Data by Class")
                class_counts = combined_df['class'].value_counts()
                st.bar_chart(class_counts)

            # Visualize by source
            if 'source' in combined_df.columns:
                st.subheader("Data by Source")
                source_counts = combined_df['source'].value_counts()
                st.bar_chart(source_counts)

            # Price distribution if available
            if 'price' in combined_df.columns:
                st.subheader("Price Distribution")
                fig, ax = plt.subplots()
                ax.hist(combined_df['price'].dropna(), bins=20)
                st.pyplot(fig)

            if 'display_image' in combined_df.columns:
                image_df = combined_df[combined_df['display_image'].fillna('').str.strip() != ''].copy()
                if not image_df.empty:
                    st.subheader("Book Cover Gallery")
                    selected_source = st.selectbox(
                        "Filter images by source",
                        ["All"] + sorted(image_df['source'].dropna().unique().tolist())
                    )
                    if selected_source != "All":
                        image_df = image_df[image_df['source'] == selected_source]

                    selected_status = st.selectbox(
                        "Filter images by status",
                        ["All"] + sorted(image_df['image_status'].dropna().unique().tolist())
                    )
                    if selected_status != "All":
                        image_df = image_df[image_df['image_status'] == selected_status]

                    selected_theme = st.selectbox(
                        "Filter images by theme",
                        ["All"] + sorted(image_df['theme'].dropna().unique().tolist())
                    )
                    if selected_theme != "All":
                        image_df = image_df[image_df['theme'] == selected_theme]

                    available_images = len(image_df)
                    default_images = min(1000, available_images)
                    max_images = st.slider(
                        "Number of images to display",
                        min_value=1,
                        max_value=available_images,
                        value=default_images
                    )
                    gallery_df = image_df.head(max_images)
                    columns = st.columns(3)

                    for index, (_, row) in enumerate(gallery_df.iterrows()):
                        with columns[index % 3]:
                            st.image(row['display_image'], caption=row['title'], width="stretch")
                            st.caption(
                                f"{row.get('author', 'Unknown')} | "
                                f"{row.get('theme', 'General')} | "
                                f"Quality {row.get('quality_score', 'N/A')} | "
                                f"Reads {row.get('read_count', 0)} | "
                                f"Orders {row.get('order_count', 0)} | "
                                f"Bought {row.get('bought_count', 0)} | "
                                f"{row.get('appreciation', 'Average')} | "
                                f"{row.get('image_status', 'Unknown')}"
                            )
        else:
            st.error("No data found in processed directory.")
    else:
        st.error("No CSV files found in processed directory.")
else:
    st.error("Processed directory does not exist. Run the ETL pipeline first.")

st.markdown("---")
st.markdown("To run this dashboard: `streamlit run dashboard.py`")
