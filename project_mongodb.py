from pymongo import MongoClient

# Подключение к базе данных
client = MongoClient('mongodb://91.190.239.132:27027/')
db = client['SHAD111_v8']

def paste_info_clients(client_id,name, surname, otchestvo, balance):
    collection = db['clients']
    
    client_data = {
        'client_id':client_id,
        'name': name,
        'surname': surname,
        'otchestvo': otchestvo,
     
        'balance': balance
    }
    
    collection.insert_one(client_data)

# Функция добавления информации о водителях в коллекцию 'Drivers'
def paste_info_drivers(auto_id,model, nomber, max_weight, st_pust,st_grug):
    collection = db['autos']
    
    driver_data = {
        'auto_id':auto_id,
        'model': model,
        'nomber': nomber,
        'max_weight': max_weight,
        'st_pust': st_pust,
        'st_grug': st_grug
    }
    
    collection.insert_one(driver_data)

# Функция добавления информации о маршрутах в коллекцию 'Route'
def add_trip(id_poezdki, client_id, auto_id, kol_pust, kol_gruzh, date,weight):
    clients_collection = db['clients']
    drivers_collection = db['autos']
    trips_collection = db['trips']
    
    client_data = clients_collection.find_one({'client_id': client_id})
    driver_data = drivers_collection.find_one({'auto_id': auto_id})
    
    if client_data and driver_data:
        if driver_data['max_weight'] > weight:
            cash = client_data['balance']
            cost = kol_pust*driver_data['st_pust']+ kol_gruzh*driver_data['st_pust']
            driver = client_data['name']+' '+ client_data['surname']+' ' + client_data['otchestvo']
            auto = str(driver_data['auto_id'])+' : '+str(driver_data['model'])+' : '+str(driver_data['nomber'])
            if cash >= cost:
                new_balance = cash - cost
                clients_collection.update_one({'client_id': client_id}, {'$set': {'balance': new_balance}})
                
                trip_data = {
                'id_poezdki': id_poezdki,
                'client_id': client_id,
                'auto_id': auto_id,
                'kol_pust': kol_pust,
                'kol_gruzh': kol_gruzh,
                'date': date,
                'weight': weight
            }
                
                trips_collection.insert_one(trip_data)
                
                print(f"Добавлена поездка пассажира {driver} на авто {auto}, со стоимостью {cost}, дата поездки {date}")
                print('--------------------------------') 
            else:
                print(f"У клиента {driver} недостаточно средств, текущий баланс: {cash}, стоимость поездки: {cost}")
                print() 
        else:
            print('Недопустимый вес',driver_data['max_weight'],' ', weight)
            print() 
    else:
        print("Неправильные данные.")
        print() 
        
    
    
def add_balance(money, client_id):
    collection = db['clients']
    
    client_data = collection.find_one({'client_id': client_id})
    if client_data:
        if money <= 99999.99:
            new_balance = client_data['balance'] + money
            collection.update_one({'client_id': client_id}, {'$set': {'balance': new_balance}})
        
            surname = client_data['name']+' '+ client_data['surname']+' ' + client_data['otchestvo']
            print(f"Баланс {surname} пополнен на сумму {money} руб.")
            print('--------------------------------') 
            print() 
        else:
            print("Вы ввели слишком большую сумму!")
            print() 
    else:
        print(f"Клиент с ID {client_id} не найден.")  
        print() 
        
def initdb():
    paste_info_clients(1,'Александр', 'Кудряшов', 'Сергеевич',  22600)
    paste_info_clients(2,'Тимур', 'Салаев', 'Эльдарович',  21434)
    paste_info_clients(3,'Алексей', 'Сушенцев', 'Артемович',  22228)
    paste_info_clients(4,'Дамир', 'Хужаев', 'Растямович',  2343)
    paste_info_clients(5,'Леонель', 'Мэсси', 'Иванович',  600000)
    
    paste_info_drivers(1,'Оконнер', '12f3e', 1250, 100,300)
    paste_info_drivers(2,'Рено', '4tvee', 1600, 600,300)
    paste_info_drivers(3,'Логан', 'werfe', 1300, 500,500)
    paste_info_drivers(4,'Дамир', 'преf3e', 11000, 300,400)
    paste_info_drivers(5,'Мэрс', 'пуп3f3e', 1500, 200,300)
    
    
  
def makejob():
    add_trip(1,1,1,2,1,'2023:12:21',100)
    add_trip(2,1,2,2,1,'2022:06:21',200)
    add_trip(3,1,3,2,1,'2023:05:21',300)
    add_balance(100, 1)
    add_trip(4,3,1,2,1,'2015:02:21',100)
    add_trip(5,2,2,2,1,'2003:01:21',200)
    add_trip(6,4,3,2,1,'2013:11:21',300)
    add_balance(1000, 4)
    add_trip(7,5,4,2,1,'2021:10:21',100)
    add_trip(8,3,5,2,1,'2022:05:21',200)
    add_trip(9,4,1,2,1,'2024:02:21',300)
    add_balance(1000, 2)
    add_trip(10,2,1,2,1,'2023:04:21',100)
    add_trip(11,2,3,2,1,'2021:03:21',200)
    add_trip(12,3,5,2,1,'2022:07:21',300)
    
def clear_collections():
    trips_collection = db['clients']
    trips_collection.delete_many({})

    trips_collection = db['autos']
    trips_collection.delete_many({})
    
    trips_collection = db['trips']
    trips_collection.delete_many({})

    
    print("Данные из коллекций успешно очищены.")
print('Введите цифру 1 для очистки базы, и её перезаполнения')
i = int(input())
while i != 1:
    print('Ошибка ввода')
    print()
    print('Введите цифру 1 для очистки базы, и её перезаполнения')
    i = int(input())
else:
    clear_collections()
    print("==============================")
    print()
    initdb()
    print()
    print('Даннык об авто и водителях обновлены')
    print()
    print("==============================")
    print()
    print('Введите цифру 2 для очистки базы, и её перезаполнения')
    i = int(input())
    makejob()
    print()
    print('Даннык о поездках обновлены')
    print()
    print("==============================")
    