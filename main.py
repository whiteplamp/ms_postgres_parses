import requests
import base64

from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table, String, Integer, Column, Float

from config import LOGIN, PASSWORD


def get_api_key():
    message = LOGIN + ':' + PASSWORD
    login_bytes = message.encode('ascii')
    base64_bytes_login = base64.b64encode(login_bytes)
    base64_message = base64_bytes_login.decode('ascii')
    print(base64_message)
    header = {"Authorization": f"Basic {base64_message}"}
    key_ = requests.post("https://online.moysklad.ru/api/remap/1.2/security/token", headers=header)
    print(key_)
    key = 'Bearer ' + key_.json()['access_token']
    return key


def get_json():
    url = 'https://online.moysklad.ru/api/remap/1.2/entity/demand'
    headers = {'Authorization': get_api_key()}
    info = requests.get(url=url, headers=headers)
    print(info.json())
    products = []
    for el in info.json()['rows']:
        date = el['updated']
        data = requests.get(url=el['positions']['meta']['href'], headers=headers)
        assortment = data.json()['rows']
        for el_ in assortment:
            assortment_data = requests.get(url=el_['assortment']['meta']['href'], headers=headers)
            assortment_data_json = assortment_data.json()
            name = assortment_data_json['name']
            price = float(el_['price']) / 100
            discount = int(el_['discount'])
            quantity = int(el_['quantity'])
            final_price = price * quantity * (1 - float(discount / 100))

            product = {
                'date': date,
                'name': name,
                'price': price,
                'discount': discount,
                'quantity': quantity,
                'final_price': final_price
            }
            products.append(product)
    print(products)
    return products


def check_table(products_json):
    engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost/test")
    conn = engine.connect()
    metadata = MetaData()

    products = Table('products', metadata,
                     Column('id', Integer(), primary_key=True),
                     Column('name', String()),
                     Column('price', Float()),
                     Column('discount', Integer()),
                     Column('quantity', Integer()),
                     Column('date', String()),
                     Column('final_price', Float()),)

    metadata.create_all(engine)

    for el in products_json:
        ins = products.insert().values(
            name=el['name'],
            price=el['price'],
            discount=el['discount'],
            quantity=el['quantity'],
            date=el['date'],
            final_price=el['final_price'],
        )
        print(ins)
        conn.execute(ins)


def main():
    check_table(get_json())


if __name__ == '__main__':
    main()
