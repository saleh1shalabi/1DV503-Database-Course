"The author of this code is Saleh Shalabi with student account ss225bx and Emma Lövgren with student account el222wg "




# Imports
##############################

import mysql.connector 
from mysql.connector import errorcode
import csv

# Connection details
############################## 

db = mysql.connector.connect(user = 'root', password= 'root', host='127.0.0.1') 

# This worked in linux but not in windows
# the problem we got was that it can't use a database with capitelletters, it will be "shalabi" and not "Shalabi"
# in linux it worked without any problem

database = "Shalabi"



# Functions
############################## 

def checking_db(db1, DB):
    # function that checks if ther is a database with the given name
    
    global path, path2 
    
    try:
        db1.execute(f"use {DB}")
        print()
        print(f"The program is now using database {DB}")

        db1.execute("show tables")  # to check if the database is empty
        print("")
        
        # if the x is 0 after the loop means ther is no tabels in database
        x = 0
        for c in db1:
            x+=1

        # if it's empty create tabels
        if x == 0:
            print("There is thow no data in this databases!")
            print("")   
            print("Trying to insert data... ")
            print("")
            
            create_tables(db1, path)
            create_tables(db1, path2)

    except Exception as e:
        # if there is no database with the given name, create new one
        if str(e) == (f"1049 (42000): Unknown database '{DB}'"):
            print(f"There is no database with name {DB}")
    
            print(f"creating database {DB}... ")
            db1.execute(f"create database {DB} default character set 'utf8'")
             
            db1.execute(f"use {DB}")

            create_tables(db1, path)
            create_tables(db1, path2)
        else:
            # if another fault print it 
            print(e)
    
    print("")
    print("===========")
    print("")


def create_tables(db1 ,filen):
    # function that creat tables in database of the given csv file


    try:

        with open(f"{filen}.csv", newline='') as csvfile:
            read = csv.reader(csvfile, delimiter=',')
            # take the first row in the file, there is what attributs it is to the table
            for row in read:
                too = []
                for c in row:
                    too.append(c)
                break # break here when the first row is looped
            
            
    
    ## error handling
    except FileNotFoundError:
        print("The files planets and speices is not in this directory!")
        print("if they are please try to open the workspecie in your IDE from the directory this and the other files are in")
        print("")
        exit()
    except Exception as e:
        print("Something went wrong")
        print(e)
        exit()
    
    
    
    x = 0
   
    for c in too:


        #  in this case the columns values will be assumed to strings no longer than 100 char
        # it is  not optimal 
        # but it works here for the given task




        if x == 0:
            # if its the first column to add to the table
            # then ther is no table yet 
            # create one and add the first column here 

            creat_table = f"create table {filen} ({c} varchar(100))" # assuming that the values is only strings and not longer the 100 char
            print(f"create table {filen}....")
            
        else:
            # when its not the first column to add to the table
            # then the table excist allrady
            # only add the column
            creat_table = f"Alter TABLE {filen} ADD COLUMN {c} varchar(100)" # assuming that the values is only strings and not longer the 100 char
        try:
            # execute the query
            print(f"adding column {c}... ",end = " ")
            db1.execute(creat_table)
        

        # Error handling if there is already a column with same name
        except mysql.connector.Error as err:
        
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR or err.errno == errorcode.ER_DUP_FIELDNAME:
                print("already exists.")
            else:
                print(err.msg)
        
        else:
            print("done")
        x+=1
    print("")
    print("===========")
    print("")


    insert_into_table(db1, filen) # insert data to the table 

    check_for_prim(db1, filen) # checks for primary key
    

def insert_into_table(db1, table):

    # function to add the data from file to table in database
     

    with open(f"{table}.csv", newline='') as csvfile:
        read = csv.reader(csvfile, delimiter=',')
        x = 0

        lista= []
        for row in read:
            values = []
            if x == 0:
                # if its the first row from file then its the columns name add it to a diffrent list

                for c in row:
                    lista.append(c)
                x+=1
                continue
            else:
                # if its not the first row 
                # add it to list values
                for c in row:
                    # append it as str 
                    # every time in the loop the list will have new data to add
                    values.append(str(c))
                

            try: 


                ################
               # this script will do same thing as following 
               # x = "%s, %s, %s, %s, %s, %s, %s, %s" 
               #   this (%s) as many as the values list length   
                x = "%s, " * len(values)
                x = x[::-1]
                x = x[2:]
                x = x[::-1]
                ###############


                # here it will be as following
                # y = f"insert into {table} values(%s, %s, %s, %s, %s, %s, %s, %s)"
                y = f"insert into {table} values({x})"

                values = tuple(values) # makes it easyer to execute query so no need to loop and add this >>>> ( )

                db1.execute(y, values) # index 0 in values will take the first %s and index 1 will take the seconde and so on 

                print("Adding new data... ", end = " ")
            
            except mysql.connector.Error as err:
                print(err.msg)
            else:
                print("New data have been added")
    print("")
    print("===========")
    print("")


def check_for_prim(db1, table):    

    # check if there is a primary ke yin the given table

    db1.execute(f"SHOW KEYS FROM {table} WHERE Key_name = 'PRIMARY'")
    x = 0
    for c in db1:
        x+=1
    # if it is 0 there is no prim key then 
    if x == 0:
        ## I could ask for iy 
        # ask = input(f"Give primary key for table {table}, (Y) for yes or any key to skip:...  ")
        
        # but I assum there will be
        ask = "y"
        
        if ask == "y" or ask == "Y":
            add_prim_key(db1, table) # add the primary key
            print(f"Primary key have been added to table {table}")
            print()
    
        else:
            print()


def add_prim_key(db1, table):
    ## show the columns in the table


    # print(f"choose the primary key for table {table}")
    # db1.execute(f"show columns from {table}")
    # for  c in db1:
    #     print(c[1], end="  ")
    # print()
   
    # ask user what column will be primary key
    # prim = str(input("what do you choose? ... : "))
    

    # here I assum it is name
    prim = "name"

    db1.execute(f"alter table {table} add PRIMARY KEY ({prim})")


def run(db1, menu):

    """ this function is the main loop of the program"""


    # variable that will be user's choose
    choose = ""
    
    
    while choose != "0":
        print(menu)
        choose = input("Choose from menu:...    ")
        print()
        
        if choose == "1":
            # show all planets in database 
            print()
            print("All planets")
            print("================")
            print()
            
            db1.execute("select name from planets") 

            # i colud just loop and print it all 
            # but the following script is only to be printed in a nicer way           
            nr = 0
            x = 0
            for c in db1:    
                if x == 5:
                    print("")
                    x = 0
                ff = len(c[0])
                print(c[0], end = " "*(20-ff)) 
                nr+=1
                x += 1
                
            
            print()
            print()
            print("Total number of planets is : ", nr)
            print()

            input("Press for return to menu ...     ")
            print("=================================")
            



        elif choose == "2":

            print("")
           
            name = ""
            # check if there is input
            while name == "" or name.isspace() :
                print("Please enter a name.... ")
                print()
                name = input("Enter the name of the planet: ...  ")
                print()
            ## this query gets only columns name but in alpabetic order 
            # db1.execute(f"SELECT column_name FROM information_schema.columns WHERE  table_name = 'planets'    AND table_schema = '{database}' ")
            
            ## this query gets columns name but gets information about it to 
            db1.execute("show columns from planets")
            
            lista = []
            # creat a list of columns name for printing later 
            for c in db1:
                lista.append(c[0]) # only the column's name 
                continue
            print()
        
            db1.execute(f"select * from planets where name = '{name}'")
            
            x = 0
            # check if any data have been found
            for c in db1:
                x+=1
            if x == 0:
                print(f"There is no planet with the name {name.capitalize()} in the database!")    
            
            x = 0

            # the loop will remove the execution of the query 
            # execute agin
            db1.execute(f"select * from planets where name = '{name}'")


            for c in db1:
                for f in lista:
                    print(lista[x].capitalize(), ":" , c[x].capitalize()) # in each index will find the column name and value in same position in each list 
                    x += 1 
                    
            print()      
            input("Press for return to menu ... ")
            print("=================================")



        elif choose == "3":

            print("")
            
            # check for right input from user

            while True:
                average_height = (input("Enter hight... "))
                print("")

                try:
                    average_height = int(average_height)
                    if average_height <= 0:
                        print("Please enter a positive number")
                        print()
                        continue

                    else: break  
                    
                except Exception: 
                    print("Please enter right values!")
                    print()     
                    

            average_height = str(average_height)
            # it will shows only species with hight greater than the given input
            # is it was descriped in the task
            db1.execute(f"select name, average_height from species where average_height > {average_height}")
            
            # controller to know how meny species it was 
            nr = 0

            for c in db1:
                ff = len(c[0])
                print("Species: ",c[0], end = " "*(20-ff))
                print("|   Average Height: ", c[1] )
                nr+=1
            print()
        
            print("Total number of speices ", nr)

            print()

            input("Press for return to menu ... ")
            print("=================================")


        elif choose == "4":

            print("")
            
            species_name = input("What is the species: ...     ")
            db1.execute(f"select planets.climate from planets, species where species.name = '{species_name}' and species.homeworld = planets.name ")
            x = 0
            # check if data have been found or not 
            for c in db1:
                x+=1
            
            if x == 0:
                print()

                print("There is no speices with the given name found in this database.")
                print()

            else:
                db1.execute(f"select planets.climate from planets, species where species.name = '{species_name}' and species.homeworld = planets.name ")
                
                for c in db1:

                    if c[0] == "NA":
                        # if there is no data about the given species
                        print()
                        print(f"Species {species_name.capitalize()} is missing this data, Climate = null")

                    else:
                        print()
                        print(f"Species {species_name.capitalize()} most likely to desire climates as {c[0].capitalize()}.")
                
                print()

            input("Press for return to menu ... ")
            
            print("=================================")

        
        elif choose == "5":

            print("")
            # there is species with classifications with no averge life and there is species with avrege lif but with no classification
            # the following query need both values to not be NA or Null to count it
            db1.execute(f'select classification, AVG(average_lifespan) from species where average_lifespan <> "NA" and classification != "NA" GROUP BY classification')
            
            print("")
            print("The average lifespan per species classification is:")
            print("")
            for c in db1:

                if c[1] == 0:
                    print(c[0].capitalize(), ": Indefinite") # the artificial species have no averge 
                    continue
                
                print(c[0].capitalize(), ":", c[1])
            print("")
            input("Press for return to menu ... ")
            
            print("=================================")
            
        else:
            continue


# Veriebels
############################## 

path = 'planets'
path2 = 'species'
menu = """ 
        Menu

        1. list all planet
        2. Search for planet details. 
        3. Search for species with height higher than given number.
        4. What is the most likely desired climate of the given species?
        5. What is the average lifespan per species classification? 
        0. Quit!
        
       """
db1 = db.cursor()



# The Program
############################## 

checking_db(db1, database)
run(db1, menu)



# Exit statment
############################## 

print("Exiting....  Bye")
