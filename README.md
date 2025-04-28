# python-database-mastery

A structured walkthrough of the **"Python Database Mastery: Dive into SQLAlchemy & Alembic"** Udemy course — including notes, examples, projects, and personal insights.

---

## 🔗 Course Link

- [Python Database Mastery: Dive into SQLAlchemy & Alembic on Udemy](https://www.udemy.com/share/108PnU3@ZOto5uc5Jn_0Z5TIakdwXypUI9L-AIzYZNMhKQXNdlWeHmr6p7qd_NsD6mqU1evd2Q==/)

  *(Note: You must be enrolled to access the full content.)*

---

## 📝 Working with Notebooks and Scripts (Jupytext)

This project uses [Jupytext](https://jupytext.readthedocs.io/en/latest/) to keep `.ipynb` notebooks and `.py` scripts synchronized.

---

### 🔄 Convert a Notebook (`.ipynb`) to a Script (`.py`)

```bash
pipenv run jupytext --to py <your_notebook_name>.ipynb
```

**Example:**

```bash
pipenv run jupytext --to py 01_connect_to_database.ipynb
```

---

### 🔄 Convert a Script (`.py`) to a Notebook (`.ipynb`)

```bash
pipenv run jupytext --to notebook <your_script_name>.py
```

**Example:**

```bash
pipenv run jupytext --to notebook 01_connect_to_database.py
```
