import requests
import base64

from sqlalchemy import create_engine, text
from sqlalchemy import MetaData, Table, String, Integer, Column, Float

from config import LOGIN, PASSWORD


def get_api_key():
    message = LOGIN + ':' + PASSWORD
    login_bytes = message.encode('ascii')
    base64_bytes_login = base64.b64encode(login_bytes)
    base64_message = base64_bytes_login.decode('ascii')
    return f'Basic {base64_message}'


def get_json():
    url = 'https://online.moysklad.ru/api/remap/1.2/entity/demand'
    headers = {'Authorization': get_api_key()}
    info = requests.get(url=url, headers=headers)
    products = []
    for el in info.json()['rows']:
        date = el['updated']
        data = requests.get(url=el['positions']['meta']['href'], headers=headers)
        assortment = data.json()['rows']
        counterparty = requests.get(el['agent']['meta']['href'], headers=headers).json()
        for el_ in assortment:
            assortment_data = requests.get(url=el_['assortment']['meta']['href'], headers=headers)
            assortment_data_json = assortment_data.json()
            try:
                name = assortment_data_json['name']
            except Exception as error:
                name = '-'
            price = float(el_['price']) / 100
            discount = int(el_['discount'])
            quantity = int(el_['quantity'])
            barcode = assortment_data_json["barcodes"][0]['ean13']
            try:
                code = assortment_data_json['code']
            except Exception as e:
                code = '-'
            try:
                article = assortment_data_json['article']
            except Exception as e:
                article = '-'
            final_price = price * quantity * (1 - float(discount / 100))
            counterparty_name = counterparty['name']
            product = {
                'date': date,
                'product_name': name,
                'counterparty_name': counterparty_name,
                'price': price,
                'discount': discount,
                'quantity': quantity,
                'code': code,
                'article': article,
                'barcode': barcode,
                'cash': 0.0,
                'non_cash': final_price
            }
            products.append(product)
    info = requests.get('https://online.moysklad.ru/api/remap/1.2/entity/retaildemand',
                        headers=headers)

    for el in info.json()['rows']:
        date = el['updated']
        data = requests.get(url=el['positions']['meta']['href'], headers=headers)
        assortment = data.json()['rows']
        counterparty = requests.get(el['agent']['meta']['href'], headers=headers).json()
        cash_sum = el['cashSum']
        non_cash_sum = el['noCashSum']
        for el_ in assortment:
            print(1)

            assortment_data = requests.get(url=el_['assortment']['meta']['href'], headers=headers)
            assortment_data_json = assortment_data.json()
            try:
                name = assortment_data_json['name']
                barcode = assortment_data_json["barcodes"][0]['ean13']
            except Exception as error:
                name = '-'
                barcode = '-'
            price = float(el_['price']) / 100
            discount = int(el_['discount'])
            quantity = int(el_['quantity'])
            try:
                code = assortment_data_json['code']
            except Exception as e:
                code = '-'
            try:
                article = assortment_data_json['article']
            except Exception as e:
                article = '-'
            try:
                counterparty_name = counterparty['name']
            except Exception as error:
                counterparty_name = '-'
            product = {
                'date': date,
                'product_name': name,
                'counterparty_name': counterparty_name,
                'price': price,
                'discount': discount,
                'quantity': quantity,
                'code': code,
                'article': article,
                'barcode': barcode,
                'cash': cash_sum,
                'non_cash': non_cash_sum
            }
            products.append(product)
    print("End of first part")
    return products


def check_table(products_json):
    engine = create_engine("postgresql+psycopg2://admin-site:postgres@localhost/admin-site")
    conn = engine.connect()
    metadata = MetaData()

    products = Table('products', metadata,
                     Column('id', Integer(), primary_key=True),
                     Column('product_name', String()),
                     Column('counterparty_name', String()),
                     Column('price', Float()),
                     Column('discount', Integer()),
                     Column('quantity', Integer()),
                     Column('date', String()),
                     Column('code', String()),
                     Column('barcode', String()),
                     Column('article', String()),
                     Column('cash', String()),
                     Column('non_cash', String()),
                     )

    metadata.create_all(engine)
    sql = text('DELETE FROM products')
    engine.execute(sql)
    for el in products_json:
        ins = products.insert().values(
            product_name=el['product_name'],
            counterparty_name=el['counterparty_name'],
            price=el['price'],
            discount=el['discount'],
            quantity=el['quantity'],
            barcode=el['barcode'],
            article=el['article'],
            code=el['code'],
            date=el['date'],
            cash=el['cash'],
            non_cash=el['non_cash']
        )
        conn.execute(ins)
        print("End of second part")


def main():
    check_table(get_json())


if __name__ == '__main__':
    main()
