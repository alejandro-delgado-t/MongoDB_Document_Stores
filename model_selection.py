# coding=utf-8
from model1 import Model1
from model2 import Model2
from model3 import Model3
import sys
from tabulate import tabulate


# Function to display available modeling options
def show_options():
    print("Choose the option you want to execute:")
    print("\t 0 - Exit")
    print("\t 1 - Model 1 (Person documents referencing Company)")
    print("\t 2 - Model 2 (Person documents with embedded Company)")
    print("\t 3 - Model 3 (Company documents with embedded Persons)")


show_options()
op = int(input("Enter option: "))

# While loop to keep running until exit option chosen
while op != 0:
    if op in [1, 2, 3]:
        # Ask for user input 
        n_people = int(input("Insert the number of employees to create: "))
        n_companies = int(input("Insert the number of companies to create: "))

        # Instantiate selected model based on input
        if op == 1:
            m = Model1()
        elif op == 2:
            m = Model2()
        elif op == 3:
            m = Model3()

        m.data_generator(n_people=n_people, n_companies=n_companies)

    else:
        # If 0 exit the program
        print("Invalid option. Exiting ...")
        sys.exit()

    # Show options again after execution
    show_options()
    op = int(input("Enter option: "))