from pymongo import MongoClient

# Подключение к базе данных
client = MongoClient('mongodb://91.190.239.132:27027/')
db = client['SHAD111_v10']

# Функция добавления информации о клиентах в коллекцию 'Clients'
def paste_info_clients(first_name, last_name, middle_name, client_id, balance):
    collection = db['Clients']
    
    client_data = {
        'first_name': first_name,
        'last_name': last_name,
        'middle_name': middle_name,
        'client_id': client_id,
        'balance': balance
    }
    
    collection.insert_one(client_data)

# Функция добавления информации о водителях в коллекцию 'Drivers'
def paste_info_drivers(first_name, last_name, middle_name, driver_id):
    collection = db['Drivers']
    
    driver_data = {
        'first_name': first_name,
        'last_name': last_name,
        'middle_name': middle_name,
        'driver_id': driver_id
    }
    
    collection.insert_one(driver_data)

# Функция добавления информации о маршрутах в коллекцию 'Route'
def paste_info_route(from_city, to_city, distance, cost, comission, route_id):
    collection = db['Route']
    
    route_data = {
        'from_city': from_city,
        'to_city': to_city,
        'distance': distance,
        'cost': cost,
        'comission': comission,
        'route_id': route_id
    }
    
    collection.insert_one(route_data)

# Функция, которая увеличивает баланс клиента на указанную сумму
def add_balance(money, client_id):
    collection = db['Clients']
    
    client_data = collection.find_one({'client_id': client_id})
    if client_data:
        if money <= 99999.99:
            new_balance = client_data['balance'] + money
            collection.update_one({'client_id': client_id}, {'$set': {'balance': new_balance}})
            
            surname = client_data['last_name']
            
            print(f"Баланс {surname} пополнен на сумму {money} руб.")
        else:
            print("Вы ввели слишком большую сумму!")
    else:
        print(f"Клиент с ID {client_id} не найден.")

# Функция добавления информации о новой поездки в коллекцию 'Trips' 
def add_trip(trip_id, client_id, driver_id, route_id):
    clients_collection = db['Clients']
    drivers_collection = db['Drivers']
    routes_collection = db['Route']
    trips_collection = db['Trips']
    
    client_data = clients_collection.find_one({'client_id': client_id})
    driver_data = drivers_collection.find_one({'driver_id': driver_id})
    route_data = routes_collection.find_one({'route_id': route_id})
    
    if client_data and driver_data and route_data:
        cash = client_data['balance']
        cost = route_data['cost']
        fam = client_data['last_name']
        driver = driver_data['last_name']
        
        if cash >= cost:
            new_balance = cash - cost
            clients_collection.update_one({'client_id': client_id}, {'$set': {'balance': new_balance}})
            
            trip_data = {
                'trip_id': trip_id,
                'client_id': client_id,
                'driver_id': driver_id,
                'route_id': route_id
            }
            
            trips_collection.insert_one(trip_data)
            
            print(f"Добавлена поездка пассажира {fam} по маршруту номер {route_id}, со стоимостью {cost}, водитель: {driver}")
        else:
            print(f"У клиента {fam} недостаточно средств, текущий баланс: {cash}, стоимость поездки: {cost}")
    else:
        print("Не удалось найти информацию о клиенте, водителе или маршруте.")

# Функция, которая инициализирует базу данных, добавляя начальные данные о клиентах, водителях и маршрутах
def initdb():
    paste_info_clients('Александр', 'Кудряшов', 'Сергеевич', 1, 600)
    paste_info_clients('Лионель', 'Месси', None, 2, 1000)
    paste_info_clients('Криштиану', 'Роналду', None, 3, 2000)
    paste_info_clients('Иван', 'Петров', 'Александрович', 4, 500)
    paste_info_clients('Лена', 'Головач', 'Максимовна', 5, 800)
    
    paste_info_drivers('Оконнер', 'Брайн', None, 1)
    paste_info_drivers('Дизель', 'Вин', None, 2)
    paste_info_drivers('Иванов', 'Сергей', 'Васильевич', 3)
    paste_info_drivers('Шумахер', 'Михаэль', 'Михайлович', 4)
    paste_info_drivers('Максимов', 'Алексей', 'Петрович', 5)
    
    paste_info_route('Москва', 'Владимир', 200, 150, 10, 1)
    paste_info_route('Москва', 'Балашиха', 35, 100, 10, 2)
    paste_info_route('Москва', 'Иваново', 300, 200, 10, 3)
    paste_info_route('Москва', 'Сочи', 990, 600, 15, 4)
    paste_info_route('Москва', 'Рязань', 200, 200, 15, 5)

# Функция, которая позволяет очищать коллекции
def clear_collections():
    trips_collection = db['Clients']
    trips_collection.delete_many({})

    trips_collection = db['Drivers']
    trips_collection.delete_many({})
    
    trips_collection = db['Trips']
    trips_collection.delete_many({})
    
    route_collection = db['Route']
    route_collection.delete_many({})
    
    print("Данные из коллекций успешно очищены.")

# Функция выполняет серию операций добавления поездок и пополнения баланса 
def makejob():
    add_trip(1, 2, 2, 2)
    add_trip(2, 1, 1, 4)
    add_trip(3, 1, 3, 2)
    add_balance(100, 1)
    add_trip(3, 1, 3, 2)
    add_trip(4, 5, 4, 5)
    add_trip(5, 3, 5, 1)
    add_trip(6, 4, 4, 4)
    add_balance(1000, 4)
    add_trip(6, 4, 4, 4)
    add_trip(7, 2, 3, 5)
    add_trip(8, 3, 2, 4)
    add_trip(9, 5, 1, 4)
    add_trip(10, 2, 1, 2)
    add_balance(1000, 1)
    add_trip(11, 1, 1, 3)
    add_trip(12, 1, 3, 1)
    add_trip(13, 1, 4, 5)
    add_trip(14, 3, 5, 1)
    add_trip(15, 5, 4, 1)

# Функция выполняет анализ всех поездок, подсчитывая количество поездок, клиентов, водителей и вычисляет доход аггрегатора и водителя
def period_analysis():
    trips_collection = db['Trips']
    route_collection = db['Route']
    
    kolvo_trips = trips_collection.count_documents({})
    kolvo_clients = len(trips_collection.distinct('client_id'))
    kolvo_drivers = len(trips_collection.distinct('driver_id'))
    
    aggregator_profit = 0.0
    drivers_profit = 0.0
    
    for trip in trips_collection.find():
        route_id = trip['route_id']
        route_data = route_collection.find_one({'route_id': route_id})
        if route_data:
            price = route_data['cost']
            commission = route_data['comission']
            aggregator_profit += price * commission / 100
            drivers_profit += price - (price * commission / 100)
    
    print(f'Количество поездок: {kolvo_trips}')
    print(f'Количество клиентов: {kolvo_clients}') 
    print(f'Количество водителей: {kolvo_drivers}')
    print(f'Доход агрегатора: {aggregator_profit}')
    print(f'Доход водителя: {drivers_profit}')
    print("==============================")

# Функция вычисляет рейтинг водителей, подсчитывая количество поездок, общий заработок и средний заработок для каждого водителя
def drivers_rating():
    drivers_collection = db['Drivers']
    trips_collection = db['Trips']
    route_collection = db['Route']

    drivers = drivers_collection.find()
    
    for driver in drivers:
        driver_id = driver['driver_id']
        surname = driver['last_name']
        name = driver['first_name']
        patronymic = driver['middle_name']
        
        trips = trips_collection.find({'driver_id': driver_id})
        
        cnt_trips = trips_collection.count_documents({'driver_id': driver_id})
        vsego_zarabotano = 0.0
        
        for trip in trips:
            route_id = trip['route_id']
            route_data = route_collection.find_one({'route_id': route_id})
            
            if route_data:
                price = route_data['cost']
                commission = route_data['comission']
                vsego_zarabotano += price - (price * commission / 100)
        
        avg_zarabotok = vsego_zarabotano / cnt_trips
        
        print(f"Surname: {surname}")
        print(f"Name: {name}")
        print(f"Patronymic: {patronymic}")
        print(f"Count Trips: {cnt_trips}")
        print(f"Total Earnings: {vsego_zarabotano}")
        print(f"Average Earnings: {avg_zarabotok}")
        print("==============================")

# Функция вычисляет рейтинг пассажиров, подсчитывая количество поездок, общий доход и средний доход для каждого пассажира
def passenger_rating():
    clients_collection = db['Clients']
    trips_collection = db['Trips']
    route_collection = db['Route']

    clients = clients_collection.find()

    result = []

    for client in clients:
        client_id = client['client_id']
        surname = client['last_name']
        name = client['first_name']
        patronymic = client['middle_name']

        trips = trips_collection.find({'client_id': client_id})

        trips_count = trips_collection.count_documents({'client_id': client_id})
        earning = 0.0

        for trip in trips:
            route_id = trip['route_id']
            route_data = route_collection.find_one({'route_id': route_id})

            if route_data:
                price = route_data['cost']
                commission = route_data['comission']
                earning += price * commission / 100

        avg_earning = earning / trips_count

        print(f"Surname: {surname}")
        print(f"Name: {name}")
        print(f"Patronymic: {patronymic}")
        print(f"Trips Count: {trips_count}")
        print(f"Earning: {earning}")
        print(f"Avg Earning: {avg_earning}")
        print("==============================")

        
clear_collections()

print("==============================")

initdb()

makejob()

print("==============================")

print('Анализ всех поездок:')
print()
period_analysis()

print('Рейтинг водителей:')
print()
drivers_rating()

print('Рейтинг пассажиров:')
print()
passenger_rating()