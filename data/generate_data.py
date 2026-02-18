from faker import Faker
import random
import csv

fake = Faker()

employee_ids = list(range(3000))

# Step 2: Choose 3–7 IDs to duplicate
num_duplicates = random.randint(1, 12)
ids_to_duplicate = random.sample(employee_ids, num_duplicates)

# Step 3: Add duplicate entries
for dup_id in ids_to_duplicate:
    employee_ids.append(dup_id)

# Optional: Shuffle so duplicates are not grouped together
random.shuffle(employee_ids)
VALID_DOMAINS = [
    "company.com",
    "corp.com",
    "enterprise.com",
    "tech.com",
    "analytics.com"
]

INVALID_DOMAINS = ["company", "corp", "company.","analytics","tech."]

def generate_email(first, last):
    r = random.random()

    if r < 0.7:
        return f"{first}.{last}@{random.choice(VALID_DOMAINS)}"
    elif r < 0.9:
        return f"{first}.{last}@{random.choice(VALID_DOMAINS)}".upper()
    else:
        return f"{first}.{last}@{random.choice(INVALID_DOMAINS)}"

with open("employees_raw.csv", "w", newline="") as f:
    writer = csv.writer(f)

    writer.writerow([
        "employee_id","first_name","last_name","email","hire_date",
        "job_title","department","salary","manager_id","address",
        "city","state","zip_code","birth_date","status"
    ])

    for emp_id  in employee_ids:
        first = fake.first_name()
        last = fake.last_name()

        writer.writerow([
            emp_id ,
            random.choice([first.lower(), first.upper(), first]),
            random.choice([last.lower(), last.upper(), last]),
            generate_email(first.lower(), last.lower()),
            random.choice([fake.date_between(start_date="-6y", end_date="+2y"),fake.date_between(start_date="-6y", end_date="-4y"),fake.date_between(start_date="-3y", end_date="-1y"),fake.date_between(start_date="-7y", end_date="-2y")]),
            fake.job(),
            random.choice(["IT", "it", "Analytics", "HR","SALES","Marketing", None]),
            random.choice(["$45,000", "$75,000", "65000", "$105,000","$95,000","55,000","$90000", "$74,000", "$85,000", "$225,000", "$62,100", None]),
            random.choice([random.randint(2000, 2100),random.randint(5000, 5100),random.randint(9000, 9100),random.randint(3000, 3150),None]),
            random.choice([fake.address().replace("\n", " "),fake.address().replace("\n", " "),fake.address().replace("\n", " "),fake.address().replace("\n", " "),None]),
            random.choice([fake.city(),fake.city(),fake.city(),fake.city(),fake.city(),None]),
            fake.state_abbr(),
            fake.postcode(),
            fake.date_of_birth(minimum_age=21, maximum_age=60),
            "Active"
        ])