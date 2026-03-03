# Publish to GitHub

## 1. Create the repository on GitHub

- Go to [github.com/new](https://github.com/new)
- **Repository name:** `super_investors`
- **Description:** (optional) e.g. "Fetch, rank, and analyze super-investor track records from Dataroma"
- Choose **Public**
- Do **not** add a README, .gitignore, or license (this project already has them)
- Click **Create repository**

## 2. Turn this folder into a Git repo and push

From your machine, in a terminal:

```bash
cd /path/to/tenachine/super_investors_open

# Initialize a new repo (only the contents of this folder)
git init

# Add all files (stats.zip is included; stats/ is in .gitignore)
git add .
git status   # optional: check what will be committed

# First commit
git commit -m "Initial commit: fetch, rank, analyze super-investors"

# Rename branch to main (if needed)
git branch -M main

# Add your GitHub repo as remote (replace YOUR_USERNAME with your GitHub username)
git remote add origin https://github.com/YOUR_USERNAME/super_investors.git

# Push
git push -u origin main
```

## 3. If you use SSH instead of HTTPS

```bash
git remote add origin git@github.com:YOUR_USERNAME/super_investors.git
git push -u origin main
```

## 4. Later: update the repo

After changing code or refreshing `stats.zip`:

```bash
cd /path/to/tenachine/super_investors_open
git add .
git commit -m "Your message"
git push
```

---

**Note:** This folder can live inside your existing `tenachine` project and still be its own Git repo (nested repo). Pushing only updates the `super_investors` repo on GitHub, not the whole tenachine project.
