import mysql.connector
from mysql.connector import errorcode
import sys

TABLES = {}

TABLES['Experiment'] = (
    "CREATE TABLE `Experiment` ("
    "  `ExperimentID` VARCHAR(10) NOT NULL,"
    "  `ManagerID` CHAR(6) NOT NULL,"
    "  `StartDate` DATE NOT NULL,"
    "  `DataEntryDate` DATE NOT NULL,"
    "  PRIMARY KEY (`ExperimentID`))")

TABLES['ParametersTypes'] = (
    "CREATE TABLE `ParametersTypes` ("
    "  `ExperimentID` VARCHAR(10) NOT NULL,"
    "  `ParameterName` VARCHAR(10) NOT NULL,"
    "  `Type` VARCHAR(10) NOT NULL,"
    "  `Required` BOOLEAN NOT NULL,"
    "  PRIMARY KEY (`ExperimentID`,`ParameterName`),"
    "CONSTRAINT paramfk FOREIGN KEY (ExperimentID) REFERENCES Experiment(ExperimentID))")

TABLES['ResultTypes'] = (
    "CREATE TABLE `ResultTypes` ("
    "  `ExperimentID` VARCHAR(10) NOT NULL,"
    "  `ResultName` VARCHAR(10) NOT NULL,"
    "  `Type` VARCHAR(10) NOT NULL,"
    "  `Required` BOOLEAN NOT NULL,"
    "  PRIMARY KEY (`ExperimentID`,`ResultName`),"
    "CONSTRAINT resultfk FOREIGN KEY (ExperimentID) REFERENCES Experiment(ExperimentID))")

TABLES['Runs'] = (
    "CREATE TABLE `Runs` ("
    "  `ExperimentID` VARCHAR(10) NOT NULL,"
    "  `TimeOfRun` DATETIME NOT NULL,"
    "  `ExperimenterSSN` VARCHAR(6) NOT NULL,"
    "  `Success` BOOLEAN NOT NULL,"
    "  PRIMARY KEY (`ExperimentID`,`TimeOfRun`),"
    "CONSTRAINT runsfk FOREIGN KEY (ExperimentID) REFERENCES Experiment(ExperimentID))")

TABLES['RunsParameter'] = (
    "CREATE TABLE `RunsParameter` ("
    "  `ExperimentID` VARCHAR(10) NOT NULL,"
    "  `TimeOfRun` DATETIME NOT NULL,"
    "  `ParameterName` VARCHAR(10) NOT NULL,"
    "  `Value` VARCHAR(10) NOT NULL,"
    "  PRIMARY KEY (`ExperimentID`,`TimeOfRun`,`ParameterName`))")

TABLES['RunsResult'] = (
    "CREATE TABLE `RunsResult` ("
    "  `ExperimentID` VARCHAR(10) NOT NULL,"
    "  `TimeOfRun` DATETIME NOT NULL,"
    "  `ResultName` VARCHAR(10) NOT NULL,"
    "  `Value` VARCHAR(10) NOT NULL,"
    "  PRIMARY KEY (`ExperimentID`,`TimeOfRun`))")

mydb = mysql.connector.connect(host='localhost', database='mysql', user='HW3335', passwd='PW3335')
mydb.autocommit = True
mycursor = mydb.cursor()

try:
    mycursor.execute("use Experiment")
except mysql.connector.Error as e:
    print("\n" +e.msg)



for table_name in TABLES:
    table_description = TABLES[table_name]
    try:
        print("Creating table {}: ".format(table_name), end='')
        mycursor.execute(table_description)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")


def drop():

    sql = ("SET FOREIGN_KEY_CHECKS = 0; "
           "DROP TABLE IF EXISTS Experiment; "
           "DROP TABLE IF EXISTS ParametersTypes; "
           "DROP TABLE IF EXISTS ResultTypes; "
           "DROP TABLE IF EXISTS Runs; "
           "DROP TABLE IF EXISTS RunsParameter; "
           "DROP TABLE IF EXISTS RunsResult;"
           "SET FOREIGN_KEY_CHECKS = 1; ")
    try:
        for result in mycursor.execute(sql, multi=True):
            pass
    except mysql.connector.Error as e:
        print(e.msg)
    print("Executed")

def maketables():
    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            print("Creating table {}: ".format(table_name), end='')
            mycursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")



def ExperimentEntry():
    ID = input("Please enter ExperimentID (must be unique)! ")
    ManagerID = input("Please enter ManagerID ")
    startDate = input("Please enter start date (yyyy-mm-dd) ")
    DataEntryDate = input("Please enter data entry date (yyyy-mm-dd) ")

    sql = "INSERT INTO Experiment VALUES (%s,%s,%s,%s); "
    vals = ID, ManagerID, startDate, DataEntryDate
    try:
        mycursor.execute(sql, vals)
    except mysql.connector.Error as e:
        print(e.msg)
        return
    print("executed")

    ParameterName = input("Please enter the parameter name ")
    Type = input("Please enter the data type of the parameter (int, float, string, url, time) ")
    Required = bool(input("Is the parameter required? (true/false) "))

    sql2 = "INSERT INTO ParametersTypes VALUES (%s, %s, %s, %s)"
    vals2 = ID, ParameterName, Type, Required

    try:
        mycursor.execute(sql2, vals2)
    except mysql.connector.Error as e:
        print(e.msg)
        return
    print("Executed")

    ResultName = input("Please enter the result name ")
    ResultType = input("Please enter the data type of the result (int, float, string, url, time) ")
    ResultRequired = bool(input("Is the result required? (true/false) "))

    sql3 = "INSERT INTO ResultTypes VALUES (%s,%s,%s,%s)"
    vals3 = ID, ResultName, ResultType, ResultRequired
    try:
        mycursor.execute(sql3, vals3)
    except mysql.connector.Error as e:
        print(e.msg)
        return
    print("Executed")


def RunEntry():
    ID = input("Please enter ExperimentID (must be unique)! ")
    TimeOfRun = input("Please enter TimeOfRun (yyyy-mm-dd) ")
    ExperimenterSSN = input("Please enter ExperimenterSSN ")
    Success = bool(input("Please enter Success (t-f) "))

    sql = "INSERT INTO Runs VALUES (%s,%s,%s,%s); "
    vals = ID, TimeOfRun, ExperimenterSSN, Success

    try:
        mycursor.execute(sql, vals)
    except mysql.connector.Error as e:
        print(e.msg)
        return
    print("Executed")

    ParameterName = input("Please enter the parameter name ")
    Value = input("Please enter the data of the parameter ")

    sql2 = "INSERT INTO RunsParameter VALUES (%s, %s, %s, %s)"
    vals2 = ID, TimeOfRun, ParameterName, Value

    try:
        mycursor.execute(sql2, vals2)
    except mysql.connector.Error as e:
        print(e.msg)
        return
    print("Executed")

    ResultName = input("Please enter the result name ")
    ResultValue = input("Please enter the value of the result ")

    sql3 = "INSERT INTO RunsResult VALUES (%s, %s, %s, %s)"
    vals3 = ID, TimeOfRun, ResultName, ResultValue
    try:
        mycursor.execute(sql3, vals3)
    except mysql.connector.Error as e:
        print(e.msg)
        return
    print("Executed")


def ExperimentDisplay():
    ID = input("Please enter experiment you want to see ")
    sql = ('SELECT * '
           ' FROM Experiment '
           'WHERE Experiment.Experiment.ExperimentID = "%s"' % ID)
    vals = ID
    try:
        mycursor.execute(sql)
    except mysql.connector.Error as e:
        print(e.msg)
        return
    print("Executed")

    myresult = mycursor.fetchall()

    for x in myresult:
        print(x)


def RunDisplay():
    ID = input("Please enter experimentID you want to see ")
    sql = ('SELECT RunsParameter.ParameterName, RunsParameter.Value, RunsResult.ResultName, RunsResult.Value '
           'FROM RunsParameter, RunsResult '
           'WHERE RunsParameter.ExperimentID = RunsResult.ExperimentID '
           'AND RunsParameter.ExperimentID = "%s"' % ID)
    try:
        mycursor.execute(sql)
    except mysql.connector.Error as e:
        print(e.msg)
        return
    print("Executed")
    myresult = mycursor.fetchall()

    for x in myresult:
        print(x)


def html():
    ID = input("Please enter experimentID you want to see ")
    sql = ('SELECT * '
           'FROM RunsResult '
           'WHERE RunsResult.ExperimentID = "%s"' % ID)
    try:
        mycursor.execute(sql)
    except mysql.connector.Error as e:
        print(e.msg)
        return
    print("Executed")

    myresult = mycursor.fetchall()

    print("HTML has been outputed to index.html \n\n\n")

    with open("./index.html", 'w') as f:
        sys.stdout = f

        print("<!DOCTYPE html>")
        print("<html>")
        print("<body>")

        print("<h2>Experiments</h2>")

        print("<table>")

        print("\t<tr>")
        print("\t\t<th> ExperimentID </th>")
        print("\t\t<th> Time of run  </th>")
        print("\t\t<th> Result Name  </th>")
        print("\t\t<th> Value        </th>")

        print("\t</tr>")

        for x in myresult:
            print("\t<tr>")
            print("\t\t<td> %s </td>" % x[0])
            print("\t\t<td> %s </td>" % x[1])
            print("\t\t<td> %s </td>" % x[2])
            print("\t\t<td> %s </td>" % x[3])

            print("\t</tr>")

        print("</table>")

        print("</body>")
        print("</html>")

        sys.stdout = sys.__stdout__


def aggregate():
    total = 0
    count = 0
    ID = input("Please enter experimentID you want to see ")
    Date1 = input("Please enter Date 1 (yyyy-mm-dd ")
    Date2 = input("Please enter Date 2 (yyyy-mm-dd ")

    Type = input("Please enter the result name ")

    sql1 = ('SELECT SUM(Value) '
            'FROM RunsResult '
            'WHERE RunsResult.ExperimentID = "%s" '
            'AND RunsResult.TimeOfRun > "%s"'
            'AND RunsResult.TimeOfRun < "%s" '
            'GROUP BY ExperimentID' % (ID, Date1, Date2))
    try:
        mycursor.execute(sql1)
    except mysql.connector.Error as e:
        print(e.msg)
        return
    print("Executed")
    myresult = mycursor.fetchall()

    print("The total is : " + str(myresult[0][0]))

    sql2 = ('SELECT AVG(Value) '
            'FROM RunsResult '
            'WHERE RunsResult.ExperimentID = "%s" '
            'AND RunsResult.TimeOfRun > "%s"'
            'AND RunsResult.TimeOfRun < "%s" '
            'GROUP BY ExperimentID' % (ID, Date1, Date2))
    try:
        mycursor.execute(sql2)
    except mysql.connector.Error as e:
        print(e.msg)
        return
    print("Executed")
    myresult = mycursor.fetchall()
    if len(myresult) < 0:
        print("No touples matched")
        return

    print("The average is : " + str(myresult[0][0]))


def ParameterSearch():
    param = input("Please enter Parameter of experiment ")
    type = input("Please enter type of parameter ( INT, FLOAT, STRING, URL, TIME) ")

    sql = ('SELECT Experiment.ExperimentID, StartDate '
           'FROM Experiment.Experiment,ParametersTypes '
           'WHERE Experiment.ExperimentID = ParametersTypes.ExperimentID '
           'AND ParameterName = "%s" '
           'AND Type = "%s" '
           'ORDER BY StartDate' % (param, type))
    try:
        mycursor.execute(sql)
    except mysql.connector.Error as e:
        print(e.msg)
        return
    print("Executed")
    myresult = mycursor.fetchall()
    for x in myresult:
        print(x)


def compare():
    ID = input("Please enter experimentID ")
    sql = ('SELECT ParametersTypes.ParameterName, ParametersTypes.Type, ResultTypes.ResultName, ResultTypes.Type '
           'FROM Experiment.Experiment, ParametersTypes, ResultTypes '
           'WHERE Experiment.Experiment.ExperimentID = ParametersTypes.ExperimentID '
           'AND ParametersTypes.ExperimentID = ResultTypes.ExperimentID '
           'AND ParametersTypes.ExperimentID = "%s"' % ID)
    try:
        mycursor.execute(sql)
    except mysql.connector.Error as e:
        print(e.msg)
        return
    print("Executed")
    myresult = mycursor.fetchall()

    sql2 = ('Select Experiment.ExperimentID '
            'FROM Experiment.Experiment, ParametersTypes, ResultTypes '
            'WHERE ParametersTypes.ParameterName = "%s" '
            'AND ParametersTypes.Type = "%s" '
            'AND ResultTypes.ResultName = "%s"'
            'AND ResultTypes.Type = "%s"' % (myresult[0], myresult[1], myresult[2], myresult[3]))
    try:
        mycursor.execute(sql2)
    except mysql.connector.Error as e:
        print(e.msg)
        return
    print("Executed")
    myresult = mycursor.fetchall()
    for x in myresult:
        print(x)


while True:

    print("1. Experiment Entry ")

    print("2. Run Entry ")

    print("3. Display information of an experiment ")

    print("4. Display information of a run ")

    print("5. Generate experiment report (HTML) ")

    print("6 Generate aggregate report ")

    print("7. Parameter search ")

    print("8. Compare experiments ")

    print("9. Drop all tables ")

    print("10. Re-create tables ")

    print("Press any other number to exit ")


    x = input("Select what you want to do ")

    if x == "1":
        ExperimentEntry()

    elif x == "2":
        RunEntry()

    elif x == "3":
        ExperimentDisplay()

    elif x == "4":
        RunDisplay()

    elif x == "5":
        html()

    elif x == "6":
        aggregate()

    elif x == "7":
        ParameterSearch()

    elif x == "8":
        compare()

    elif x == "9":
        drop()

    elif x == "10":
        maketables()

    else:
        print("Exiting...")
        break
