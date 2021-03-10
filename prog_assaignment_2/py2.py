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
# the problem we got was that it can't use a data base with capitelletters, it will be "shalabi_lövgren" and not "Shalabi_Lövgren"
# in linux it worked without any problem

database = "testet"



# Functions
############################## 

def checking_db(db1, DB):
    # function that checks if ther is a database with the given name
    
    global pathes
    
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
            
            for c in pathes:
                create_tables(db1,c)
            
            for c in pathes:
                add_prim_key(db1,c)
                

            add_foreign(db1, pathleaders)
            add_foreign(db1, pathactivity)
            add_foreign(db1, pathmembINact)

            for c in pathes:
                insert_into_table(db1, c)    
    except Exception as e:
        # if there is no database with the given name, create new one
        if str(e) == (f"1049 (42000): Unknown database '{DB}'"):
            print(f"There is no database with name {DB}")
    
            print(f"creating database {DB}... ")
            db1.execute(f"create database {DB} default character set 'utf8'")
             
            db1.execute(f"use {DB}")

            for c in pathes:
                create_tables(db1,c)
            
            for c in pathes:
                add_prim_key(db1,c)
                

            add_foreign(db1, pathleaders)
            add_foreign(db1, pathactivity)
            add_foreign(db1, pathmembINact)

            for c in pathes:
                insert_into_table(db1, c) # insert data to the table 

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
                    if c == "":
                        values.append(None)
                    else:
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
                db.commit()
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
        ask = input(f"Give primary key for table {table}, (Y) for yes or any key to skip:...  ")
        
        # but I assum there will be
        # ask = "y"
        
        if ask == "y" or ask == "Y":
            add_prim_key(db1, table) # add the primary key
            print(f"Primary key have been added to table {table}")
            print()
    
        else:
            print()

def add_foreignAsk(table):  
    x = input(f"Add foreign key to {table}?  ")  

    if x == "y" or x == "Y":
        return True
    else:
        return False
        
def add_prim_key(db1, table):
    ## show the columns in the table
    pathmembINact = "memInAct"
    

    print(f"choose the primary key for table {table}")
    db1.execute(f"show columns from {table}")
    for  c in db1:
        print(c[0], end="  ")
    print()
   
    # ask user what column will be primary key
    # prim = str(input("what do you choose? ... : "))
    
    if table == pathmembINact:
        prim = "activityID, memberID"
    else:
        prim = "ID"
    # # here I assum it is name
    # prim = ID

    db1.execute(f"alter table {table} add PRIMARY KEY ({prim})")

def add_foreign(db1,table):
    
    pathleaders = 'leaders'
    pathactivity = "activity"
    pathmembINact = "memInAct"
    

    print(f"choose the foreign key for table {table}")
    db1.execute(f"show columns from {table}")
    for  c in db1:
        print(c[0], end="  ")
    print()
   
    # ask user what column will be primary key
    # foreign = str(input("what do you choose? ... : "))
    
    # ref = str(input("referenc table:::: "))
    # ref2 = str(input("referenc column::.. "))
    # # here I assum it is name
    # prim = "name"


    if table == pathleaders:
        foreign = "ID"
        ref = "members"
        ref2 = "ID"
    elif table == pathactivity:
        foreign = "leader"
        ref = pathleaders
        ref2 = "ID"
        db1.execute(f"alter table {table} add foreign KEY ({foreign}) references  {ref}({ref2})")
        foreign = "transID"
        ref = "economy"
        ref2 = "ID"
    else:
        foreign = "memberID"
        ref = "members"
        ref2 = "ID"
        db1.execute(f"alter table {table} add foreign KEY ({foreign}) references  {ref}({ref2})")
        foreign = "activityID"
        ref = pathactivity
        ref2 = "ID"

    db1.execute(f"alter table {table} add foreign KEY ({foreign}) references  {ref}({ref2})")




# Veriebels
############################## 

pathmembers = 'members'
pathleaders = 'leaders'
pathactivity = "activity"
pathmembINact = "memInAct"
patheconomy = 'economy'

pathes =  [pathmembers, pathleaders, patheconomy, pathactivity, pathmembINact]



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
# run(db1, menu)
db1.execute("update activity, economy set activity.transID = economy.ID where activity.ID = economy.activityID")
db.commit()
# Exit statment
############################## 

print("Exiting....  Bye")