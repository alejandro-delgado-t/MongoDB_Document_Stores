# coding=utf-8
import time
import random
from pymongo import MongoClient
from faker import Faker
from datetime import datetime
from tabulate import tabulate


class Model1:
    """
    Generates synthetic Company and Person documents stored in 'lab2_big_data' collection.
    Companies and persons are separate documents.
    Each person references their company via company_id.
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
        fake = Faker(["it_IT", "es_ES", "en_US"])

        # --- Generate companies and insert them -------------------------------
        companies = []
        for _ in range(n_companies):
            # Generate fake company data
            company = {
                "type": "company",
                "domain": fake.domain_name(),
                "email": fake.company_email(),
                "name": fake.company(),
                "url": fake.url(),
                "vatNumber": fake.bothify(text="??########"),
            }
            companies.append(company)

        # Insert data
        result = collection.insert_many(companies)
        company_ids = result.inserted_ids
        print(f"Inserted {len(company_ids)} Documents for companies successfully")

        # --- Generate persons with even distribution across companies ---------
        employees_per_company = n_people // n_companies
        extra_employees = n_people % n_companies  # remainder to distribute

        # Insert track time
        t0 = time.time()
        buffer = []
        inserted_total = 0

        for i, company_id in enumerate(company_ids):
            # Determine number of employees for this company
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
                    "company_id": company_id,
                }
                buffer.append(person)

                # Insert in batches
                if len(buffer) >= batch_size:
                    collection.insert_many(buffer)
                    inserted_total += len(buffer)
                    print(f"{inserted_total:,} / {n_people:,} Documents for persons inserted successfully")
                    buffer.clear()

        # Insert any remaining documents in the buffer
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
        result = list(collection.aggregate([
            { "$match": { "type": "person" } },
            {
                "$lookup": {
                    "from": "lab2_big_data",
                    "localField": "company_id",
                    "foreignField": "_id",
                    "as": "company"
                }
            },
            { "$unwind": "$company" },
            { "$project": {
                "_id": 0,
                "fullName": 1,
                "companyName": "$company.name"
            }}
        ]))
        query_time = time.time() - start_time
        print("Q1 Result--- %.4f seconds ---" % query_time)
        
        table = [[entry['fullName'], entry['companyName']] for entry in result[:10]]
        print(tabulate(table, headers=["Full Name", "Company Name"], tablefmt="grid"))

        # Q2: For each company, retrieve its name and the number of employees
        start_time = time.time()
        result = list(collection.aggregate([
            { "$match": { "type": "company" } },
            {
                "$lookup": {
                    "from": "lab2_big_data",
                    "localField": "_id",
                    "foreignField": "company_id",
                    "as": "employees"
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "companyName": "$name",
                    "employeeCount": { "$size": "$employees" }
                }
            }
        ]))
        query_time = time.time() - start_time
        print("Q2 Result--- %.4f seconds ---" % query_time)
        
        table = [[entry['companyName'], entry['employeeCount']] for entry in result[:10]]
        print(tabulate(table, headers=["Company Name", "Number of employees"], tablefmt="grid"))

        # Q3: For each person born before 1988, update their age to 30
        cutoff = datetime(1988, 1, 1)

        start_time = time.time()
        result = collection.update_many(
            {
                "type": "person",
                "dateOfBirth": { "$lt": cutoff }
            },
            {
                "$set": { "age": 30 }
            }
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
            { "type": "company" },
            [{ "$set": { "name": { "$concat": ["$name", " Company"] } } }]
        )
        query_time = time.time() - start_time
        print("Q4 Result--- %.4f seconds ---" % query_time)
        
        updated_companies = collection.find(
            { "type": "company" },
            { "_id": 0, "name": 1 }
        ).limit(10)
        table = [[doc["name"]] for doc in updated_companies]
        print(tabulate(table, headers=["Updated Company Name"], tablefmt="grid"))

# Leaving the option to run the script directly
if __name__ == "__main__":
    generator = Model1()
    generator.data_generator()