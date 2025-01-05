def get_items_data(items: list) -> list:
    all_items = list()
    for item in items:
        item_data = dict()
        item = item['body']
        item_data['category_id'] = item["category_id"]
        item_data['price'] = item["price"]
        item_data['seller_id'] = item["seller_id"]
        item_data['title'] = item["title"]

        item_data['free_shipping'] = item['shipping']['free_shipping']
        item_data['local_pick_up'] = item['shipping']['local_pick_up']
        item_data['logistic_type'] = item['shipping']['logistic_type']
        item_data['shipping_mode'] = item['shipping']['mode']
    
        get_warranties_date(item_data, item['sale_terms'])
        
        all_items.append(item_data)

    return all_items



def get_warranties_date(item: dict, warranties_data: list) -> None:
    for warranty in warranties_data:
        if warranty["id"] == 'WARRANTY_TYPE':
            item['warranty_type'] = warranty['value_name']

        if warranty["id"] == 'WARRANTY_TIME':
            item['warranty_time'] = warranty['value_name']