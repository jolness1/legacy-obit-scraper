# License Search Application

This project is designed to search for obituaries of licensed nursing and physician professionals in Montana. It iterates through CSV files containing license data and constructs search queries to find relevant obituary entries.

## Project Structure

```
├── legacy-obit-scraper
│── main.py
│── main-parallel.py
│── nursing-licenses.csv    # CSV file containing nursing license data
│── physician-licenses.csv   # CSV file containing physician license data
│── possibilities.csv       # File to log found obituary results
├── requirements.txt            # List of project dependencies
├── README.md                   # Project documentation
└── venv/                       # Virtual environment for dependencies
```

## Setup Instructions

1. **Clone the repository**:
   ```
   git clone <repository-url>
   cd license-search-app
   ```

2. **Create a virtual environment**:
   ```
   python -m venv venv
   ```

3. **Activate the virtual environment**:
   - On Windows:
     ```
     venv\Scripts\activate
     ```
   - On macOS/Linux:
     ```
     source venv/bin/activate
     ```

4. **Install dependencies**:
   ```
   pip install -r requirements.txt
   ```

## Usage Guidelines

1. Ensure that the `nursing-licenses.csv` and `physician-licenses.csv` files are populated with the necessary data.
2. Run the main script:
   ```
   python src/main.py
   ```
3. Check the `possibilities.txt` file for any obituary results that were found during the search.

## Notes

- The project is designed to err on the side of false positives, meaning it will log any potential matches found during the search.
- Ensure you have internet access while running the script, as it performs Google searches for each entry.
