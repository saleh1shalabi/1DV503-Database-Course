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
        print("The files are not in this directory!")
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
        
def add_prim_key(db1, table):
    ## show the columns in the table
    pathmembINact = "memInAct"
   
    
    #all tables have ID as a prim but not memInAct    
    if table == pathmembINact:
        prim = "activityID, memberID"
    else:
        prim = "ID"

    db1.execute(f"alter table {table} add PRIMARY KEY ({prim})")
    print(f"")

def add_foreign(db1,table):
    
    pathleaders = 'leaders'
    pathactivity = "activity"
    

    print(f"choose the foreign key for table {table}")
    db1.execute(f"show columns from {table}")
    for  c in db1:
        print(c[0], end="  ")
    print()

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
            print("the members")
            print("================")
            print()
            
            db1.execute("SELECT firstName, lastName, ID "
                        "FROM members, memInAct "
                        "WHERE members.ID = memInAct.memberID "
                        "GROUP BY ID") 

            # i colud just loop and print it all 
            # but the following script is only to be printed in a nicer way           
            
            print("firstName", end =" "*(20-len("firstName")))
            print("lastName", end =" "*(20-len("lastName")))
            print("ID", end =" "*(20-len("ID")))
            print()
            nr = 0
            for c in db1:
                print()    
                print(c[0], end = " "*(20-len(c[0])))
                print(c[1], end = " "*(20-len(c[1]))) 
                print(c[2],end = "") 
                nr+=1
            
                
                
            
            print()
            print()
            print("Total number of members that have at least one activity is : ", nr)
            print()
            print()      

            input("Press for return to menu ...     ")
            print("=================================")
            



        elif choose == "2":
            print()
            print("Sum the cost of all activities for each leader")
            print("================")
            print()
    
        
            db1.execute("SELECT leader as ID, firstName, lastName, sum(cost) as sumOfCost "
                        "FROM economy, members, activity "
                        "WHERE economy.ID = activity.transID AND activity.leader = members.ID "
                        "GROUP BY  activity.leader")
            
            
            print("ID", end =" "*(20-len("ID")))
            print("firstName", end =" "*(20-len("firstName")))
            print("lastName", end =" "*(20-len("lastName")))
            print("sumOfCost", end =" "*(20-len("sumOfCost")))
            print()



            for c in db1:
                print()    
                print(c[0], end = " "*(20-len(c[0])))
                print(c[1], end = " "*(20-len(c[1]))) 
                print(c[2], end = " "*(20-len(c[2])))
                print(c[3], end = "") 
                
            print()      
            print()      
            input("Press for return to menu ... ")
            print("=================================")



        elif choose == "3":

            print("")
            
            # check for right input from user

            while True:
                menAct = (input("How meny activitys ? ... "))
                print("")

                try:
                    menAct = int(menAct)
                    if menAct <= 0:
                        print("Please enter a positive number")
                        print()
                        continue

                    else: break  
                    
                except Exception: 
                    print("Please enter right values!")
                    print()     
                    


            # it will shows only species with hight greater than the given input
            # is it was descriped in the task
            db1.execute("SELECT firstName, lastName, activitys "
                        "FROM members " 
                        "JOIN (SELECT COUNT(activityID) AS activitys, memberID "
                        "FROM memInAct " 
                        "GROUP BY memberID) AS particByMem ON particByMem.memberID = ID "
                        f"WHERE activitys > {menAct}")
            
            

            
            print("firstName", end =" "*(20-len("firstName")))
            print("lastName", end =" "*(20-len("lastName")))
            print("activitys", end =" "*(20-len("activitys")))
            print()


            for c in db1:
                print()    
                print(c[0], end = " "*(20-len(c[0])))
                print(c[1], end = " "*(20-len(c[1]))) 
                print(c[2], end = "")
                
                
                    
            print()
            print()      
            input("Press for return to menu ... ")
            print("=================================")



        elif choose == "4":

            print()
            print("The activity with most members")
            print("================")
            print()
    
            try:        
                db1.execute("CREATE VIEW memParAct AS SELECT COUNT(memberID) AS membersParAct, activityID, name, leader "
                            "FROM memInAct "
                            "JOIN activity ON activity.ID = activityID "
                            "GROUP BY activityID")
            except Exception:
                pass
            db1.execute("SELECT membersParAct, activityID, name, leader AS leaderID, firstName, lastName "
                        "FROM memParAct "
                        "JOIN members ON members.ID = leader "
                        "WHERE membersParAct = (SELECT MAX(membersParAct) FROM memParAct)")
            
            print("membersParAct", end =" "*(20-len("membersParAct")))
            print("activityID", end =" "*(20-len("activityID")))
            print("activityName", end =" "*(20-len("activityName")))
            print("leaderID", end =" "*(20-len("leaderID")))
            print("firstName", end =" "*(20-len("firstName")))
            print("lastName", end =" "*(20-len("lastName")))
            print()
            


            for c in db1:
                print()
                print(c[0], end = " "*(20-len(str(c[0])))) 
                print(c[1], end = " "*(20-len(str(c[1]))))
                print(c[2], end = " "*(20-len(str(c[2])))) 
                print(c[3], end = " "*(20-len(str(c[3]))))
                print(c[4], end = " "*(20-len(str(c[4])))) 
                print(c[5], end = " "*(20-len(str(c[5])))) 
                

                
                
            print()         
            print()      
            input("Press for return to menu ... ")
            print("=================================")

        
        elif choose == "5":

            print()
            print("Leaders not leading any activity")
            print("================")
            print()
            
            db1.execute("SELECT firstName, lastName, members.ID AS ID "
                        "FROM members "
                        "JOIN (SELECT * FROM leaders WHERE ID  NOT IN (SELECT leader FROM activity)) AS leadersNoLead ON leadersNoLead.ID = members.ID") 

            # i colud just loop and print it all 
            # but the following script is only to be printed in a nicer way           
            
            print("firstName", end =" "*(20-len("firstName")))
            print("lastName", end =" "*(20-len("lastName")))
            print("ID", end =" "*(20-len("ID")))
            print()

            nr = 0
            for c in db1:
                print()    
                print(c[0], end = " "*(20-len(c[0])))
                print(c[1], end = " "*(20-len(c[1]))) 
                print(c[2],end = "")
                nr+=1

            
                
                
            
            print()
            print()
            print("Total number of leaders not leading any activity is : ", nr)
            print()
            print()      

            input("Press for return to menu ...     ")
            print("=================================")
        elif choose == "6":
            
            print("")
            
            print("""
                1.  To see sumOfCost par activity name
                2.  To specify an activity
                0.  To get back """)
            print()
            print()

            choose2 = input("what you choose?... ")
            while choose2 != "0":
                if choose2 == "1":
                    print("")
                    
                    db1.execute("SELECT name AS activityName, SUM(cost) AS totalCost "
                            "FROM activity, economy "
                            "WHERE economy.ID = activity.transID "
                            "GROUP BY activity.name")
                
                

                
                    print("activityName", end =" "*(20-len("activityName")))
                    print("totalCost", end =" "*(20-len("totalCost")))
                    print()

                    


                    for c in db1:
                        print()    
                        print(c[0], end = " "*(20-len(str(c[0]))))
                        print(c[1], end = " "*(20-len(str(c[1])))) 
                        
                        
                            
                    print()
                    print()      
                    input("Press for return to menu ... ")
                    print("=================================")
                    choose2 = "0"

                elif choose2 == "2":
                    actname = (input("What activity? ... "))
                    print("")

                            
                            


                    # it will shows only species with hight greater than the given input
                    # is it was descriped in the task
                    db1.execute("SELECT name AS activityName, SUM(cost) AS totalCost "
                                "FROM activity, economy "
                                f"WHERE economy.ID = activity.transID AND activity.name = '{actname}' ")
                    
                    

                    
                    print("activityName", end =" "*(20-len("activityName")))
                    print("totalCost", end =" "*(20-len("totalCost")))
                    print()



                    for c in db1:
                        print()    
                        print(c[0], end = " "*(20-len(str(c[0]))))
                        print(c[1], end = " "*(20-len(str(c[1])))) 
                        
                        
                            
                    print()
                    print()      
                    input("Press for return to menu ... ")
                    print("=================================")
                    choose2 = "0"

                
                else: continue
        else: continue



# Veriebels
############################## 

pathmembers = 'members'
patheconomy = 'economy'
pathleaders = 'leaders'
pathactivity = "activity"
pathmembINact = "memInAct"

pathes =  [pathmembers, pathleaders, patheconomy, pathactivity, pathmembINact]

menu = """ 
        Menu

        1. List all names of the members that participate in an activity
        2. Sum the cost of all activities for each leader 
        3. List all names of the members that have participated in more than (X) activities.
        4. Select the activity name and the leader’s name for the activity with most members in it
        5. Select the leaders that don’t leads any activity
        6. Select the sum of Amount for all activity with name (X)
        0. Quit!
        
       """
db1 = db.cursor()



# The Program
############################## 

checking_db(db1, database)
db1.execute("update activity, economy set activity.transID = economy.ID where activity.ID = economy.activityID")
db1.execute("alter table economy add unique(activityID) ")
db.commit()

run(db1,menu)
# Exit statment
############################## 

print("Exiting....  Bye")