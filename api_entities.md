Список полей:
    - идентификатор курьера
    - имя курьера
    - дата заказа
    - сумма заказа
    - рейтинг курьера
    - сумма чаевых

Список необходимых таблиц, которые есть в слое dds:
    - dm_orders
    - dm_timestamps
    - fct_product_sales

Список таблиц которые необходимо создать в слое dds:
    - dm_couriers
    - dm_deliveries
    - fct_delivery_of_orders

Cписок сущностей и полей, которые необходимо загрузить из API:
    /couriers
        - _id
        - name
    /deliveries
        order_id
        delivery_id
        courier_id
        address
        delivery_ts
        rate
        tip_sum