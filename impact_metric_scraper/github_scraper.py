import requests
import pandas as pd
import os
from dotenv import load_dotenv # Import load_dotenv

# --- Load environment variables from .env file --
# --- Configuration ---
GITHUB_TOKEN = os.getenv("GITHUB_API_KEY")

BASE_URL = "https://api.github.com"
HEADERS = {
    "Accept": "application/vnd.github.v3+json",
    "Authorization": f"token {GITHUB_TOKEN}"
}

def get_repos_by_topic(topic, per_page=100):
    """
    Fetches repositories associated with a given topic using the GitHub Search API.
    Handles pagination.
    """
    all_repos_data = []
    page = 1
    while True:
        url = f"{BASE_URL}/search/repositories?q=topic:{topic}&per_page={per_page}&page={page}"
        #print(f"Fetching page {page}...")
        response = requests.get(url, headers=HEADERS)

        if response.status_code == 200:
            data = response.json()
            items = data.get("items", [])
            all_repos_data.extend(items)

            # Check for pagination
            if "next" in response.links:
                page += 1
            else:
                break
        else:
            print(f"Error fetching data: {response.status_code} - {response.text}")
            break
    return all_repos_data

def get_repo_details(owner, repo_name):
    """
    Fetches detailed metadata for a specific repository.
    """
    url = f"{BASE_URL}/repos/{owner}/{repo_name}"
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching details for {owner}/{repo_name}: {response.status_code} - {response.text}")
        return None

def main(TOPIC):
    #print(f"Searching for repositories with topic: '{TOPIC}'")
    repos_summary = get_repos_by_topic(TOPIC)
    #print(f"Found {len(repos_summary)} repositories with topic '{TOPIC}'.")

    if not repos_summary:
        print("No repositories found for the given topic.")
        return

    # Extract relevant metadata and store in a list of dictionaries
    repo_metadata_list = []
    for repo_sum in repos_summary:
        # The search API provides a good amount of metadata directly.
        # You can enrich this with more detailed calls if needed.
        repo_data = {
            "name": repo_sum.get("name"),
            "full_name": repo_sum.get("full_name"),
            "owner_login": repo_sum.get("owner", {}).get("login"),
            "description": repo_sum.get("description"),
            "html_url": repo_sum.get("html_url"),
            "clone_url": repo_sum.get("clone_url"),
            "stars": repo_sum.get("stargazers_count"),
            "forks": repo_sum.get("forks_count"),
            "language": repo_sum.get("language"),
            "created_at": repo_sum.get("created_at"),
            "updated_at": repo_sum.get("updated_at"),
            "pushed_at": repo_sum.get("pushed_at"),
            "license": repo_sum.get("license", {}).get("spdx_id") if repo_sum.get("license") else None,
            "has_issues": repo_sum.get("has_issues"),
            "has_projects": repo_sum.get("has_projects"),
            "has_downloads": repo_sum.get("has_downloads"),
            "has_wiki": repo_sum.get("has_wiki"),
            "homepage": repo_sum.get("homepage"),
            "topics": repo_sum.get("topics", [])
        }
        repo_metadata_list.append(repo_data)

    # Convert to Pandas DataFrame
    df = pd.DataFrame(repo_metadata_list)
    print(f"\nDataFrame successfully created for {TOPIC}: {df.shape[0]} rows captured")
    return df

if __name__ == "__main__":
   open_app_github_df = main("scripture-open-apps")
   open_components_github_df = main("scripture-open-components")

def collect_metrics() -> dict:
    """
    Runs the GitHub scrapers for scripture-open-apps and scripture-open-components
    and returns summary counts.
    """
    # Re-use your main() to pull the two topics
    open_app_github_df = main("scripture-open-apps")
    open_components_github_df = main("scripture-open-components")

    if open_app_github_df is None or open_components_github_df is None:
        return {
            "open_app_count": None,
            "regional_apps_count": None,
            "open_components_count": None,
            "os_org_count": None,
            "status": "error",
            "error_message": "One or both GitHub queries returned no data."
        }

    open_app_count = len(open_app_github_df)
    regional_apps_count = len(open_app_github_df[open_app_github_df['owner_login'] != 'unfoldingWord'])
    open_components_count = len(open_components_github_df)

    unique_creators = set(open_components_github_df['owner_login'])
    # Defensive: only remove if present
    unique_creators.discard('unfoldingWord-box3')
    os_org_count = len(unique_creators)

    return {
        "open_app_count": open_app_count,
        "regional_apps_count": regional_apps_count,
        "open_components_count": open_components_count,
        "os_org_count": os_org_count,
    }
