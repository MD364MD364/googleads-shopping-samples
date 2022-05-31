from __future__ import print_function
from asyncio.windows_events import NULL
import sys

import mariadb
import json
import bigcommerce
import requests
from copy import copy
# The common module provides setup functionality used by the samples,
# such as authentication and unique id generation.
from shopping.content import common

requests = []

def submitRequest(requestBody):
    requests.append(requestBody)
    if(len(requestBody) >= 100):
        startCount = len(re)
        service, config, _ = common.init(argv, __doc__)
        merchant_id = config['merchantId']

        batch = {
            'entries': [{
                'batchId': i,
                'merchantId': merchant_id,
                'method': 'insert',
                'product': sample.create_product_sample(
                    config,
                    'book#%s' % common.get_unique_id(),
                    title='This is book number %d' % i,
                    price={
                        'value': '%d.50' % i,
                        'currency': 'USD',
                    }),
                } for i in range(BATCH_SIZE)],
        }

        request = service.products().custombatch(body=batch)
        result = request.execute()

        if result['kind'] == 'content#productsCustomBatchResponse':
            entries = result['entries']
            for entry in entries:
                product = entry.get('product')
                errors = entry.get('errors')
                if product:
                    print('Product "%s" with offerId "%s" was created.' %
                        (product['id'], product['offerId']))
                elif errors:
                    print('Errors for batch entry %d:' % entry['batchId'])
                    print(json.dumps(errors, sort_keys=True, indent=2,
                                    separators=(',', ': ')))
        else:
            print('There was an error. Response: %s' % result)

def main(argv):

    api = bigcommerce.api.BigcommerceApi(client_id='gnw0dh7xzx97ck9pw4kwpthvefui412', store_hash='z84xkjcnbz', access_token='tqhvp7fmyqr438pewjwtcwi1vggxpky')
    # print(api.Products.get(21742).variant())

    v3client = bigcommerce.connection.OAuthConnection(client_id='gnw0dh7xzx97ck9pw4kwpthvefui412',
                                                      store_hash='z84xkjcnbz',
                                                      access_token='tqhvp7fmyqr438pewjwtcwi1vggxpky',
                                                      api_path='/stores/{}/v3/{}')

    # Connect to MariaDB Platform
    try:
        conn = mariadb.connect(
            user="root",
            password="5LUxZA2CnEmZQ8dm",
            host="127.0.0.1",
            port=3306,
            database="safetymediaapp"
        )
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)

    totalSubmission = 0
    # Get Cursor
    cur = conn.cursor()
    # cur.execute("SELECT * FROM product WHERE statusMerchantCenter='2' AND visible_udf_data LIKE \'%\"brandname\"\:\"Safety Media\"%\' AND custom_fields LIKE '%\"googlemcavail\"\:true%'")
    # cur.execute("SELECT * FROM product WHERE statusMerchantCenter='2' AND custom_fields LIKE '%\"googlemcavail\"\:true%'")
    cur.execute("SELECT * FROM product WHERE custom_fields LIKE '%\"googlemcavail\"\:true%'")
    productsAll = []
    count = 0
    for (prod) in cur:
        count += 1
        productsAll.append(prod)
    print(count)
    for prod in productsAll:
        print('Handling ' + prod[5])
        custom_fields = json.loads(prod[22])

        # If parent, needs to be ignored. Only single products and variants need to go up
        if(custom_fields['primaryselector'] != prod[5]):
            # it's not a parent
            # Let's create a base product, then modify for each scenario
            productGeneral = {
                'offerId': prod[5],
                'title': prod[3],
                'description': custom_fields['prodhtml'],
                'contentLanguage': 'en', # apparently refers to page language and we only have english, so en it is for now
                'channel': 'online',
                'availability': 'in stock',
                'condition': 'new',
                'sellOnGoogleQuantity': 11,
                'mpn': prod[5],
                'minHandlingTime': int(json.loads(prod[22])['RegProdDel']),
                'maxHandlingTime': (json.loads(prod[22])['prodelrange'] + int(json.loads(prod[22])['RegProdDel'])),
                'productWeight': {'value': prod[16], 'unit': 'lb'}
            }

            # If brandname exists in spire, set it
            if(('brandname' in json.loads(prod[20])) and (json.loads(prod[20])['brandname'] != "")):
                productGeneral['brand'] = json.loads(prod[20])['brandname']
            # If promotionIds exists, set it
            if(('googlepromoid' in json.loads(prod[20])) and (json.loads(prod[22])['googlepromoid'] != "")):
                # separates values using a comma
                productGeneral['promotionIds'] = json.loads(prod[22])['googlepromoid'].split(',')
            
            # Handle size
            sizeSet = False
            # If custom size is set, parse that
            # TODOBARDIA Need to consider custom size being in cm or mm as well. Have to check if that even exists
            if((json.loads(prod[22])['size'] != "") and (('"W' in json.loads(prod[22])['size']) or ('mmW' in json.loads(prod[22])['size'])) and (('"H' in json.loads(prod[22])['size']) or ('mmH' in json.loads(prod[22])['size']))):
                # print('there is custom size')
                a = 0
                dimensions = json.loads(prod[22])['size'].split('x')
                for dimension in dimensions:
                    dimUnit = ''
                    if('"' in dimension):
                        dim = dimension.split('"')[0].lstrip().rstrip()
                        dimUnit = 'in'
                    elif('mm' in dimension):
                        dim = dimension.split('mm')[0].lstrip().rstrip()
                        dimUnit = 'mm'
                    # print(dim)
                    match a:
                        case 0:
                            # 'set width'
                            # print('set width')
                            if dimUnit == 'in':
                                productGeneral['productWidth'] = {'value': dim, 'unit': 'in'}
                            elif dimUnit == 'mm':
                                productGeneral['productWidth'] = {'value': dim/10, 'unit': 'cm'}
                            sizeSet = True
                        case 1:
                            #'set height'
                            # print('set height')
                            if dimUnit == 'in':
                                productGeneral['productHeight'] = {'value': dim, 'unit': 'in'}
                            elif dimUnit == 'mm':
                                productGeneral['productHeight'] = {'value': dim/10, 'unit': 'cm'}
                        case 2:
                            # 'set depth'
                            # print('set depth')
                            if dimUnit == 'in':
                                productGeneral['productLength'] = {'value': dim, 'unit': 'in'}
                            elif dimUnit == 'mm':
                                productGeneral['productLength'] = {'value': dim/10, 'unit': 'cm'}
                    a+=1
                if(a == 2):
                    # means there was no depth. Need to grab depth from dimensions tab of the UDFs
                    if(json.loads(prod[22])['depth'] != NULL):
                        productGeneral['productLength'] = {'value': json.loads(prod[22])['depth'], 'unit': 'in'}
            elif((sizeSet == False) and (json.loads(prod[22])['primaryselector'] != "")):
                # custom size field was not filled, so we'll be using the selector reference instead
                # print('there is a primaryselector')
                # print(prod[9])
                try:
                    conn2 = mariadb.connect(
                        user="root",
                        password="5LUxZA2CnEmZQ8dm",
                        host="127.0.0.1",
                        port=3306,
                        database="safetymediaapp"
                    )
                except mariadb.Error as e:
                    print(f"Error connecting to MariaDB Platform: {e}")
                    sys.exit(1)
                # print('using second cursor')
                cur2 = conn2.cursor()
                cur2.execute(
                    "SELECT custom_fields FROM product WHERE sku='" + prod[9] + "'")
                for (parent) in cur2:
                    if('size' in json.loads(parent[0])['selectorref'].lower()):
                        position = json.loads(parent[0])['selectorref'].lower()[0 : json.loads(parent[0])['selectorref'].lower().index('size')].count(',') + 1
                        # print('Selector has size option #' + str(position))
                        # print(json.loads(prod[22])['ref' + str(position)])
                        dropdownDimensions = json.loads(prod[22])['ref' + str(position)]
                        if(('w' in dropdownDimensions.lower()) and ('h' in dropdownDimensions.lower())):
                            # at least width and length are in
                            dimCount = 0
                            dimensions = dropdownDimensions.split('x')
                            for dimension in dimensions:
                                if('"' in dimension):
                                    amount = float(dimension.split('"')[0].lstrip().rstrip())
                                    unit = 'in'
                                elif('mm' in dimension.lower()):
                                    amount = float(dimension.split('mm')[0].lstrip().rstrip()) / 10
                                    unit = 'cm'
                                elif('cm' in dimension.lower()):
                                    amount = float(dimension.split('cm')[0].lstrip().rstrip())
                                    unit = 'cm'
                                
                                match dimCount:
                                    case 0:
                                        # send width
                                        productGeneral['productWidth'] = {'value': amount, 'unit': unit}
                                        sizeSet = True
                                    case 1:
                                        # send height
                                        productGeneral['productHeight'] = {'value': amount, 'unit': unit}
                                    case 2:
                                        # send depth
                                        productGeneral['productLength'] = {'value': amount, 'unit': unit}
                                        
                                # print(str(amount) + unit)
                                dimCount += 1
            elif((sizeSet == False)):
                # Grab straight from dimensions tab of spire
                productGeneral['productWidth'] = {'value': json.loads(prod[22])['Width'], 'unit': 'in'}
                productGeneral['productHeight'] = {'value': json.loads(prod[22])['Height'], 'unit': 'in'}
                productGeneral['productLength'] = {'value': json.loads(prod[22])['depth'], 'unit': 'in'}
            else:
                print('None of the size methods worked. Something is wrong')
                quit()
            
            # Handle customAttributes
            totalCustoms = []
            if(json.loads(prod[22])['includes'] != ""):
                totalCustoms.append(json.loads(prod[22])['includes'])
            if(json.loads(prod[22])['selfadhesivesticker'] == True):
                totalCustoms.append('Self-Adhesive Sticker')
            if(json.loads(prod[22])['2sidedtape'] == True):
                totalCustoms.append('2 Sided Tape')
            if(json.loads(prod[22])['slotholes'] == True):
                totalCustoms.append('Slot Holes')
            if(json.loads(prod[22])['holes'] == True):
                totalCustoms.append('Holes')
            if(json.loads(prod[22])['screws'] == True):
                totalCustoms.append('Screws')
            if(len(totalCustoms) != 0):
                productGeneral['customAttributes'] = {"name": "Includes", "value": (", ".join(totalCustoms))}
                # productGeneral['customAttributes'] = ",".join(totalCustoms)

            
            # Handle material
            if(json.loads(prod[22])['material'] != ""):
                productGeneral['material'] = json.loads(prod[22])['material']
            else:
                # Need to check if it's a variant with a selector dropdown of material
                if(json.loads(prod[22])['primaryselector'] != ""):
                    # print('there is a primaryselector')
                    # print(prod[9])
                    try:
                        conn2 = mariadb.connect(
                            user="root",
                            password="5LUxZA2CnEmZQ8dm",
                            host="127.0.0.1",
                            port=3306,
                            database="safetymediaapp"
                        )
                    except mariadb.Error as e:
                        print(f"Error connecting to MariaDB Platform: {e}")
                        sys.exit(1)
                    cur2 = conn2.cursor()
                    cur2.execute(
                        "SELECT custom_fields FROM product WHERE sku='" + prod[9] + "'")
                    for (parent) in cur2:
                        if('material' in json.loads(parent[0])['selectorref'].lower()):
                            position = json.loads(parent[0])['selectorref'].lower()[0 : json.loads(parent[0])['selectorref'].lower().index('material')].count(',') + 1
                            # print('Selector has material option #' + str(position))
                            # print(json.loads(prod[22])['ref' + str(position)])
                            productGeneral['material'] = json.loads(prod[22])['ref' + str(position)]



            # check if variant
            if(prod[4] is not None):
                print('Variant')

                # need to figure out the link from querying the parent\
                try:
                    conn3 = mariadb.connect(
                        user="root",
                        password="5LUxZA2CnEmZQ8dm",
                        host="127.0.0.1",
                        port=3306,
                        database="safetymediaapp"
                    )
                except mariadb.Error as e:
                    print(f"Error connecting to MariaDB Platform: {e}")
                    sys.exit(1)

                # Second cursor to pull the needed info from the parent
                cur2 = conn3.cursor()
                cur2.execute(
                    "SELECT custom_fields, bigc_product_id FROM product WHERE sku='" + prod[9] + "'")
                for (parent) in cur2:
                    variantURLCA = 'https://safetymedia.com/' + json.loads(parent[0])['seourl'].replace(' ', '-').lower() + '?setCurrencyId=1&sku=' + prod[5]
                    variantURLUS = 'https://safetymedia.com/' + json.loads(parent[0])['seourl'].replace(' ', '-').lower() + '?setCurrencyId=2&sku=' + prod[5]
                    parentBigcID = parent[1]
                    # print(variantURLCA)
                    # print(variantURLUS)
                    # print(parentBigcID)
                conn3.close()

                variantBigCInfo = v3client.get('/catalog/products/' + str(parentBigcID) + '/variants/' + str(prod[4]))

                print('CA')
                variantCA = copy(productGeneral)

                variantCA['id'] = 'online:EN:CA:' + prod[5]
                variantCA['link'] = variantURLCA
                variantCA['imageLink'] =  variantBigCInfo['data']['image_url']
                variantCA['targetCountry'] = 'CA'
                variantCA['price'] = {'value': prod[7], 'currency': 'CAD'}
                variantCA['itemGroupId'] = prod[9]
                # If gtin exists, set it
                # TODOBARDIA IF BLANK,CHECK ISBN NUMBER, IF ISBN BLANK, PUT BLANK. IF EITHER EXISTS, SET IDENTIFIER TRUE,
                if(('upc' in prod[25]) and (json.loads(prod[25])['upc'] != '')):
                    variantCA['gtin'] = json.loads(prod[25])['upc']
                    variantCA['identifierExists'] = 'True'
                elif(('isbn' in prod[22]) and (json.loads(prod[22])['isbn'] != '')):
                    variantCA['gtin'] = json.loads(prod[22])['isbn']
                    variantCA['identifierExists'] = 'True'

                # Construct the service object to interact with the Content API.
                service, config, _ = common.init(argv, __doc__)

                # Get the merchant ID from merchant-info.json.
                merchant_id = config['merchantId']

                # print(variantCA)
                # Create the request with the merchant ID and product object.
                request = service.products().insert(merchantId=merchant_id, body=variantCA)

                # Execute the request and print the result.
                result = request.execute()
                print('Product with offerId "%s" was updated.' %
                      (result['offerId']))
                totalSubmission += 1

                print('US')
                variantUS = copy(variantCA)

                variantUS['id'] = 'online:EN:US:' + prod[5]
                variantUS['link'] = variantURLUS
                variantUS['targetCountry'] = 'US'
                variantUS['price'] = {'value': round(prod[7]/1.1, 2), 'currency': 'USD'}


                # Construct the service object to interact with the Content API.
                service, config, _ = common.init(argv, __doc__)

                # Get the merchant ID from merchant-info.json.
                merchant_id = config['merchantId']

                # print(variantUS)
                # Create the request with the merchant ID and product object.
                request = service.products().insert(merchantId=merchant_id, body=variantUS)

                # Execute the request and print the result.
                result = request.execute()
                print('Product with offerId "%s" was updated.' %
                      (result['offerId']))
                totalSubmission += 1

            else:
                print('Single product')
                print('CA')
                # singleProdBigCInfo = v3client.get('/catalog/products/' + str(prod[1]) + '/')
                singleProdBigCInfo = api.Products.get(prod[1])
                # print(singleProdBigCInfo)
                singleCA = copy(productGeneral)
                singleCA['id'] = 'online:EN:CA:' + prod[5]
                singleCA['link'] = 'https://safetymedia.com/' + prod[5] + '/' + json.loads(prod[22])['seourl'].replace(' ', '-').lower() + '?setCurrencyId=1'
                singleCA['imageLink'] =  singleProdBigCInfo['primary_image']['zoom_url']
                singleCA['targetCountry'] = 'CA'
                singleCA['price'] = {'value': prod[7], 'currency': 'CAD'}
                # singleCA['itemGroupId'] = prod[9]
                # If gtin exists, set it
                if(('upc' in prod[25]) and (json.loads(prod[25])['upc'] != '')):
                    singleCA['gtin'] = json.loads(prod[25])['upc']
                    singleCA['identifierExists'] = 'True'
                elif(('isbn' in prod[22]) and (json.loads(prod[22])['isbn'] != '')):
                    singleCA['gtin'] = json.loads(prod[22])['isbn']
                    singleCA['identifierExists'] = 'True'
                
                # Construct the service object to interact with the Content API.
                service, config, _ = common.init(argv, __doc__)

                # Get the merchant ID from merchant-info.json.
                merchant_id = config['merchantId']

                # print(singleCA)
                # Create the request with the merchant ID and product object.
                request = service.products().insert(merchantId=merchant_id, body=singleCA)

                # Execute the request and print the result.
                result = request.execute()
                print('Product with offerId "%s" was updated.' %
                      (result['offerId']))
                totalSubmission += 1

                print('US')
                singleUS = copy(singleCA)

                singleUS['id'] = 'online:EN:US:' + prod[5]
                singleUS['link'] = 'https://safetymedia.com/' + prod[5] + '/' + json.loads(prod[22])['seourl'].replace(' ', '-').lower() + '?setCurrencyId=2'
                singleUS['targetCountry'] = 'US'
                singleUS['price'] = {'value': round(prod[7]/1.1, 2), 'currency': 'USD'}

                # Construct the service object to interact with the Content API.
                service, config, _ = common.init(argv, __doc__)

                # Get the merchant ID from merchant-info.json.
                merchant_id = config['merchantId']

                # print(singleUS)
                # Create the request with the merchant ID and product object.
                # request = service.products().insert(merchantId=merchant_id, body=singleUS)
                request = service.products().insert(merchantId=merchant_id, body=singleUS)

                # Execute the request and print the result.
                result = request.execute()
                print('Product with offerId "%s" was updated.' %
                      (result['offerId']))
                totalSubmission += 1

        else:
            print('Parent')
    print('Total submissions: ' + str(totalSubmission))
    quit()

        # once for CA, second time for US

        # print(prod[4])
        # product = {
        #     'offerId':
        #     prod[5],
        #     'id':
        #     'online:EN:CA:' + prod[5],
        #     'title':
        #     prod[3],
        #     'description':
        #     custom_fields['prodhtml'],
        #     'link':
        #     'https://safetymedia.com/AA67/tamper-dye',
        #     'imageLink':
        #     'https://cdn11.bigcommerce.com/s-z84xkjcnbz/images/stencil/1920w/products/15839/86766/product16845__93436.1650403573.jpg?c=1',
        #     'contentLanguage':
        #     'en',
        #     'targetCountry':
        #     'CA',
        #     'channel':
        #     'online',
        #     'availability':
        #     'in stock',
        #     'condition':
        #     'new',
        #     'googleProductCategory':
        #     'Media > Books',
        #     'gtin':
        #     '9780007312896',
        #     'price': {
        #         'value': '85.59',
        #         'currency': 'CAD'
        #     },
        #     'shipping': [{
        #         'country': 'CA',
        #         'service': 'Standard shipping',
        #         'price': {
        #             'value': '1',
        #             'currency': 'CAD'
        #         }
        #     }],
        #     'shippingWeight': {
        #         'value': '200',
        #         'unit': 'grams'
        #     }
        # }
    
    

    # Construct the service object to interact with the Content API.
    service, config, _ = common.init(argv, __doc__)

    # Get the merchant ID from merchant-info.json.
    merchant_id = config['merchantId']

    # Create the request with the merchant ID and product object.
    request = service.products().insert(merchantId=merchant_id, body=product)

    # Execute the request and print the result.
    result = request.execute()
    print('Product with offerId "%s" was created.' % (result['offerId']))

    conn.close()

    productCA = {
        'offerId': prod[5],
        'id': 'online:EN:CA:' + prod[5],
        'title': prod[3],
        'description': custom_fields['prodhtml'],
        'link': variantURLCA,
        'imageLink': variantBigCInfo['data']['image_url'],
        'contentLanguage': 'en',
        'targetCountry': 'CA',
        'channel': 'online',
        'availability': 'in stock',
        'condition': 'new',
        'gtin': '9780007312896',
        'price': {
            'value': prod[7],
            'currency': 'CAD'
        },
        'shipping': [{
            'country': 'CA',
            'service': 'Standard shipping',
            'price': {
                'value': '1',
                'currency': 'CAD'
            }
        }],
        'shippingWeight': {
            'value': '200',
            'unit': 'grams'
        }
    }


# Allow the function to be called with arguments passed from the command line.
if __name__ == '__main__':
    main(sys.argv)
