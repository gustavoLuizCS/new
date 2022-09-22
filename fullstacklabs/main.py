import pandas as pd
import pyodbc
import matplotlib.pyplot as plt

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DESKTOP-PRULHA5;'
                      'Database=PythonTest;'
                      'Trusted_Connection=yes;')


def load_data():
    # Import CSV
    data = pd.read_csv('candidates.csv', sep=';')
    df = pd.DataFrame(data)
    df.columns = df.columns.str.replace(' ', '')

    cursor = conn.cursor()
    # Create Table
    cursor.execute('''
            CREATE TABLE candidates (
                FirstName nvarchar(50),
                LastName nvarchar(50),
                Email nvarchar(50),
                ApplicationDate DATE,
                Country nvarchar(100),
                YOE nvarchar(50),
                Seniority nvarchar(50),
                Technology nvarchar(50),
                CodeChallengeScore nvarchar(50),
                TechnicalInterviewScore nvarchar(50),
                )
                   ''')

    ## Insert DataFrame to Table
    for row in df.itertuples():
        cursor.execute('''INSERT INTO candidates (FirstName, LastName, Email, ApplicationDate, Country,YOE,Seniority,
        Technology,CodeChallengeScore,TechnicalInterviewScore ) VALUES (?,?,?,?,?,?,?,?,?,?)''',
                       row.FirstName,
                       row.LastName,
                       row.Email,
                       row.ApplicationDate,
                       row.Country,
                       row.YOE,
                       row.Seniority,
                       row.Technology,
                       row.CodeChallengeScore,
                       row.TechnicalInterviewScore
                       )
    conn.commit()


def hiresbytechnology():
    sql = "select Technology,count(Technology) as suma from candidates where TechnicalInterviewScore >= 7 group by " \
          "Technology "

    data = pd.read_sql(sql, conn)
    tech_data = data["Technology"]
    suma_data = data["suma"]
    plt.pie(suma_data, labels=tech_data)
    plt.show()


def hiresbyyear():
    sql = "select year(ApplicationDate) as Year,count(applicationdate) as suma from candidates where " \
          "TechnicalInterviewScore >= 7 group by Applicationdate "

    data = pd.read_sql(sql, conn)
    tech_data = data["Year"]
    suma_data = data["suma"]
    plt.bar(x=tech_data, y=suma_data, height=1)
    plt.show()

# call this function to load data into the test database
hiresbyyear()