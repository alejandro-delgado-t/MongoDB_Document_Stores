# Lab 2: Document Stores

## Big Data Management for Data Science 
## Alejandro Delgado and Maria Aleman

---

## Description of Model Scripts

- **Model Selection (model_selection.py)**: Main interface for selecting and running models - when executed, will provide an interactive command-line interface in which the user is prompted to select a data model and feed in input parameters directly through the Python terminal.
- **Model 1 (model1.py)**: Normalized design where `Person` and `Company` are separate documents. Persons reference their company using `company_id`.
- **Model 2 (model2.py)**: Denormalized design where each `Person` document embeds their associated `Company` object.
- **Model 3 (model3.py)**: Denormalized design where each `Company` document embeds a list of `Person` documents (employees).

---

## Requirements

- Python 3.9 or higher
- MongoDB running locally (port `27017`)
- Python packages:
  - `pymongo`
  - `time`
  - `faker`
  - `random`
  - `datetime`
  - `tabulate` (optional)

Install packages with:

```bash
pip install pymongo faker tabulate
```

---

## How to Run

1. Make sure a local MongoDB instance is running.
2. Install the necessary Python packages in your environment.
3. Begin the project by executing the model_selection.py script from a command prompt.
4. When prompted, choose one of the three data-modeling options and present the requested numbers for employees and companies.
5. The script will: generate synthetic data, insert it into MongoDB, execute the four benchmark queries, and report the timing results.
6. Having completed this, opt to execute another model or exit the menu.