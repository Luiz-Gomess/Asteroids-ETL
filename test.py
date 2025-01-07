from dateutil.relativedelta import relativedelta
import datetime

current_date = datetime.date.today()
lista = [(current_date - relativedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]

print(lista)