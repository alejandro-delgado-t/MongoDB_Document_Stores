# coding=utf-8
import time
import random
from pymongo import MongoClient
from faker import Faker
from datetime import datetime
from tabulate import tabulate


class Model2:
    """
    Generates synthetic Person documents with embedded Company data stored in 'lab2_big_data' collection.
    Each Person document includes:
        - person fields
        - embedded company fields
    Distribution of employees per company is proportional and consistent.
    """

    def data_generator(
        self,
        n_people: int = 99_000,
        n_companies: int = 1_000,
        batch_size: int = 2_000,
        mongo_uri: str = "mongodb://localhost:27017/",
        db_name: str = "local",
    ) -> None:
        client = MongoClient(mongo_uri)
        db = client[db_name]

        # Clear and create collection
        db.drop_collection("lab2_big_data")
        collection = db.create_collection("lab2_big_data")

        # Initialize Faker
        fake = Faker(["es_ES", "it_IT", "en_US"])

        # --- Generate companies -----------------------------------------------
        companies = []
        for _ in range(n_companies):
            company = {
                "domain": fake.domain_name(),
                "email": fake.company_email(),
                "name": fake.company(),
                "url": fake.url(),
                "vatNumber": fake.bothify(text="??########"),
            }
            companies.append(company)

        print(f"Prepared {len(companies)} company templates")

        # --- Generate persons with embedded company ---------------------------
        employees_per_company = n_people // n_companies
        extra_employees = n_people % n_companies

        t0 = time.time()
        buffer = []
        inserted_total = 0

        for i, company in enumerate(companies):
            # Distribute people evenly among companies
            num_employees = employees_per_company + (1 if i < extra_employees else 0)
            for _ in range(num_employees):
                dob = fake.date_of_birth(minimum_age=18, maximum_age=70)
                age = (
                    datetime.now().year - dob.year
                    - ((datetime.now().month, datetime.now().day) < (dob.month, dob.day))
                )
                person = {
                    "type": "person",
                    "age": age,
                    "companyEmail": fake.company_email(),
                    "dateOfBirth": datetime.combine(dob, datetime.min.time()),
                    "email": fake.email(),
                    "firstName": fake.first_name(),
                    "fullName": fake.name(),
                    "sex": random.choice(["M", "F"]),
                    "company": company,  # embedded company info
                }
                buffer.append(person)

                # Insert in batches
                if len(buffer) >= batch_size:
                    collection.insert_many(buffer)
                    inserted_total += len(buffer)
                    print(f"{inserted_total:,} / {n_people:,} Documents for persons inserted successfully")
                    buffer.clear()

        if buffer:
            collection.insert_many(buffer)
            inserted_total += len(buffer)
            print(f"{inserted_total:,} / {n_people:,} Documents for persons inserted successfully")

        elapsed = time.time() - t0
        print(
            f"Finished – {n_people:,} persons in {elapsed:.1f}s "
            f"({n_people / max(elapsed, 1):,.0f} docs/s)"
        )
        
        
        ###################### Queries ######################################

        # Q1:  For each person, retrieve their full name and their company’s name
        start_time = time.time()
        result = list(collection.find(
            { "type": "person" },
            { "_id": 0, "fullName": 1, "company.name": 1 }
        ))
        query_time = time.time() - start_time
        print("Q1 Result--- %.4f seconds ---" % query_time)
        
        results = collection.find(
            { "type": "person" },
            { "_id": 0, "fullName": 1, "company.name": 1 }
        ).limit(10)
        table = [[doc["fullName"], doc["company"]["name"]] for doc in results]
        print(tabulate(table, headers=["Full Name", "Company Name"], tablefmt="grid"))

        # Q2: For each company, retrieve its name and the number of employees
        start_time = time.time()
        result = list(collection.aggregate([
            { "$match": { "type": "person" } },
            { "$group": {
                "_id": "$company.name",
                "employeeCount": { "$sum": 1 }
            }},
            { "$project": {
                "_id": 0,
                "companyName": "$_id",
                "employeeCount": 1
            }}
        ]))
        query_time = time.time() - start_time
        print("Q2 Result--- %.4f seconds ---" % query_time)
        
        table = [[doc["companyName"], doc["employeeCount"]] for doc in list(result)[:10]]
        print(tabulate(table, headers=["Company Name", "Employee Count"], tablefmt="grid"))

        # Q3: For each person born before 1988, update their age to 30
        cutoff = datetime(1988, 1, 1)

        start_time = time.time()
        result = collection.update_many(
            {
                "type": "person",
                "dateOfBirth": { "$lt": cutoff }
            },
            { "$set": { "age": 30 } }
        )
        query_time = time.time() - start_time
        print("Q3 Result--- %.4f seconds ---" % query_time)
        
        updated_docs = collection.find(
            {
                "type": "person",
                "dateOfBirth": { "$lt": cutoff }
            },
            { "_id": 0, "fullName": 1, "age": 1, "dateOfBirth": 1 }
        ).limit(10)
        table = [[doc["fullName"], doc["dateOfBirth"].strftime("%Y-%m-%d"), doc["age"]] for doc in updated_docs]
        print(tabulate(table, headers=["Full Name", "Date of Birth", "Age"], tablefmt="grid"))

        # Q4: For each company, update its name to include the word “Company”
        start_time = time.time()
        result = collection.update_many(
            { "type": "person" },
            [{ "$set": { "company.name": { "$concat": ["$company.name", " Company"] } } }]
        )
        query_time = time.time() - start_time

        print("Q4 Result--- %.4f seconds ---" % query_time)
        
        updated_docs = collection.find(
            { "type": "person" },
            { "_id": 0, "fullName": 1, "company.name": 1 }
        ).limit(10)
        table = [[doc["company"]["name"]] for doc in updated_docs]
        print(tabulate(table, headers=["Updated Company Name"], tablefmt="grid"))

# Leaving the option to run the script directly
if __name__ == "__main__":
    generator = Model2()
    generator.data_generator()