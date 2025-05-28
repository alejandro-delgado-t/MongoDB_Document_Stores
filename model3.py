# coding=utf-8
import time
import random
from pymongo import MongoClient
from faker import Faker
from datetime import datetime
from tabulate import tabulate


class Model3:
    """
    Generates synthetic Company documents with embedded Person documents stored in 'lab2_big_data' collection.
    Each Company document includes:
        - company fields
        - a list of embedded person documents
    Distribution of employees per company is proportional and consistent.
    """

    def data_generator(
        self,
        n_people: int = 99_000,
        n_companies: int = 1_000,
        batch_size: int = 100,
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

        employees_per_company = n_people // n_companies
        extra_employees = n_people % n_companies

        t0 = time.time()
        buffer = []
        inserted_total = 0

        for i in range(n_companies):
            # Distribute employees evenly among companies
            num_employees = employees_per_company + (1 if i < extra_employees else 0)

            # Create company document
            company = {
                "type": "company",
                "domain": fake.domain_name(),
                "email": fake.company_email(),
                "name": fake.company(),
                "url": fake.url(),
                "vatNumber": fake.bothify(text="??########"),
                "employees": [],  # list of embedded person documents
            }

            # Generate and embed person documents
            for _ in range(num_employees):
                dob = fake.date_of_birth(minimum_age=18, maximum_age=70)
                age = (
                    datetime.now().year - dob.year
                    - ((datetime.now().month, datetime.now().day) < (dob.month, dob.day))
                )
                person = {
                    "age": age,
                    "companyEmail": fake.company_email(),
                    "dateOfBirth": datetime.combine(dob, datetime.min.time()),
                    "email": fake.email(),
                    "firstName": fake.first_name(),
                    "fullName": fake.name(),
                    "sex": random.choice(["M", "F"]),
                }
                company["employees"].append(person)

            buffer.append(company)
            inserted_total += num_employees

            # Insert in batches
            if len(buffer) >= batch_size:
                collection.insert_many(buffer)
                print(f"{inserted_total:,} / {n_people:,} Employees inserted into companies")
                buffer.clear()

        if buffer:
            collection.insert_many(buffer)
            print(f"{inserted_total:,} / {n_people:,} Employees inserted into companies")

        elapsed = time.time() - t0
        print(
            f"Finished – {inserted_total:,} persons embedded in companies in {elapsed:.1f}s "
            f"({inserted_total / max(elapsed, 1):,.0f} docs/s)"
        )
        
        
        ###################### Queries ######################################
        
        # Q1:  For each person, retrieve their full name and their company’s name
        start_time = time.time()
        result = list(collection.aggregate([
            { "$unwind": "$employees" },
            { "$project": {
                "_id": 0,
                "fullName": "$employees.fullName",
                "companyName": "$name"
            }}
        ]))
        query_time = time.time() - start_time
        print("Q1 Result--- %.4f seconds ---" % query_time)
        
        coll = collection.find(
            { "type": "company" },
            { "_id": 0, "name": 1, "employees.fullName": 1 }
        )
        # Flatten company-employee pairs
        table = []
        for doc in coll:
            company_name = doc["name"]
            for emp in doc["employees"][:3]:
                table.append([emp["fullName"], company_name])
                if len(table) >= 10:
                    break
            if len(table) >= 10:
                break
        print(tabulate(table, headers=["Full Name", "Company Name"], tablefmt="grid"))

        # Q2: For each company, retrieve its name and the number of employees
        start_time = time.time()
        result = list(collection.find(
            {},
            { "_id": 0, "name": 1, "employeeCount": { "$size": "$employees" } }
        ))
        query_time = time.time() - start_time
        print("Q2 Result--- %.4f seconds ---" % query_time)
        
        results = collection.find(
            { "type": "company" },
            { "_id": 0, "name": 1, "employees": 1 }
        ).limit(10)
        table = [[doc["name"], len(doc["employees"])] for doc in results]
        print(tabulate(table, headers=["Company Name", "Employee Count"], tablefmt="grid"))


        # Q3: For each person born before 1988, update their age to 30
        cutoff = datetime(1988, 1, 1)

        start_time = time.time()
        companies = collection.find({ "employees.dateOfBirth": { "$lt": cutoff } })

        modified_count = 0

        # Since employees are embedded - update them in place
        for company in companies:
            updated_employees = []
            modified = False
            for emp in company["employees"]:
                if emp["dateOfBirth"] < cutoff:
                    emp["age"] = 30
                    modified = True
                updated_employees.append(emp)
            if modified:
                collection.update_one(
                    { "_id": company["_id"] },
                    { "$set": { "employees": updated_employees } }
                )
                modified_count += 1

        query_time = time.time() - start_time
        print("Q3 Result--- %.4f seconds ---" % query_time)
        
        updated_docs = collection.aggregate([
            { "$unwind": "$employees" },
            { "$match": { "employees.dateOfBirth": { "$lt": cutoff } } },
            { "$project": {
                "_id": 0,
                "fullName": "$employees.fullName",
                "dateOfBirth": "$employees.dateOfBirth",
                "age": "$employees.age"
            }},
            { "$limit": 10 }
        ])
        table = [
            [doc["fullName"], doc["dateOfBirth"].strftime("%Y-%m-%d"), doc["age"]]
            for doc in updated_docs
        ]
        print(tabulate(table, headers=["Full Name", "Date of Birth", "Age"], tablefmt="grid"))

        # Q4: For each company, update its name to include the word “Company”
        start_time = time.time()
        result = collection.update_many(
            {},
            [{ "$set": { "name": { "$concat": ["$name", " Company"] } } }]
        )
        query_time = time.time() - start_time
        print("Q4 Result--- %.4f seconds ---" % query_time)
        
        updated_companies = collection.find(
            {},
            { "_id": 0, "name": 1 }
        ).limit(10)

        table = [[doc["name"]] for doc in updated_companies]
        print(tabulate(table, headers=["Updated Company Name"], tablefmt="grid"))
                
# Leaving the option to run the script directly
if __name__ == "__main__":
    generator = Model3()
    generator.data_generator()