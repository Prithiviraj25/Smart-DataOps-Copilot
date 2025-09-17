from github import Github
import os
from datetime import datetime

# Config
REPO_NAME = os.environ['GITHUB_REPOSITORY']
TOKEN = os.environ['GITHUB_TOKEN']
MAX_STREAK = 10  # max commits to show

# Authenticate
g = Github(TOKEN)
repo = g.get_repo(REPO_NAME)

# Get last commits by the current user
commits = repo.get_commits(author=repo.owner.login)[:MAX_STREAK]

streak_lines = []
for commit in commits:
    date = commit.commit.author.date.strftime("%Y-%m-%d")
    msg = commit.commit.message.split("\n")[0]
    streak_lines.append(f"- **{date}**: {msg}")

streak_md = "# 📈 Streak Tracker\n\n" + "\n".join(streak_lines)

# Write to STREAK.md
with open("STREAK.md", "w") as f:
    f.write(streak_md)