from __future__ import print_function
import sys
import json
from copy import copy

import mariadb
import bigcommerce
import requests
from requests.auth import HTTPBasicAuth
from slugify import slugify

from shopping.content import common


def isValidNumber(numString):
    if len(numString) > 0:
        for c in numString:
            if (not c.isdigit()) and (c != '.'):
                return False
    else:
        return False
    return True


def safe_json_loads(value, default=None):
    if default is None:
        default = {}
    try:
        if value is None or value == "":
            return default
        return json.loads(value)
    except Exception:
        return default


def getExchangeRate():
    url = "https://white-key-5840.spirelan.com:10880/api/v2/companies/smidata/currencies/"
    username = "AUTOS"
    password = "Bardia&SafetyMedia@2021!"

    try:
        response = requests.get(url, auth=HTTPBasicAuth(username, password), verify=False)
        response.raise_for_status()
        data = response.json()

        usd_fixed_rate = None
        for record in data.get("records", []):
            if record.get("code") == "USD":
                usd_fixed_rate = record.get("fixedRate")
                break

        if usd_fixed_rate:
            print(f"The fixed rate for USD is: {usd_fixed_rate}")
            return float(usd_fixed_rate)
        else:
            print("USD rate not found.")
            return 0.0

    except requests.exceptions.RequestException as e:
        print("An error occurred while fetching the currency data:", e)
        return 0.0


def get_db_connection():
    return mariadb.connect(
        user="root",
        password="5LUxZA2CnEmZQ8dm",
        host="127.0.0.1",
        port=3306,
        database="safetymediaapp"
    )


def createItem(line):
    return {
        'bigc_product_id': line[1],
        'spire_product_id': line[2],
        'product_name': line[3],
        'variant_id': line[4],
        'sku': line[5],
        'description': line[6],
        'price': line[7],
        'categories': line[8],
        'parent_product_code': line[9],
        'is_featured': line[10],
        'availability': line[11],
        'standard_cost': line[12],
        'image_path': line[13],
        'image_url': line[14],
        'image_id': line[15],
        'weight': line[16],
        'buy_measure_code': line[17],
        'availability_description': line[18],
        'images': line[19],
        'visible_udf_data': line[20],
        'material_details': line[21],
        'custom_fields': line[22],
        'updated_custom_fields': line[23],
        'price_matrix': line[24],
        'bigc_variant_productid': line[25],
        'upcs_values': line[26],
        'spire_product_status': line[27],
        'status': line[28],
        'statusMerchantCenter': line[29],
        'created_at': line[30],
        'updated_at': line[31],
        'lastImageUpdate': line[32],
    }


def fixURL(rawURL):
    newURL = rawURL.replace(" ", "-")
    newURL = newURL.replace(",", "")
    newURL = newURL.replace(".", "")
    newURL = newURL.replace('"', "")
    newURL = newURL.replace('/', "")
    return slugify(newURL)


def get_first_bigc_image_url_from_images_response(images_response):
    data = images_response.get("data", [])
    if not data:
        return ""

    chosen = None
    for img in data:
        if img.get("is_thumbnail") is True:
            chosen = img
            break
    if chosen is None:
        chosen = data[0]

    for key in ["url_zoom", "url_standard", "url_thumbnail", "image_url", "url"]:
        if chosen.get(key):
            return chosen.get(key)

    return ""


def get_parent_product_image(v3client, parent_bigc_id):
    try:
        parentImages = v3client.get(f'/catalog/products/{parent_bigc_id}/images')
        return get_first_bigc_image_url_from_images_response(parentImages)
    except Exception as e:
        print(f"PARENT IMAGE LOOKUP FAILED for parent BigC ID {parent_bigc_id}: {type(e).__name__}: {e}")
        return ""


def get_single_product_image(v3client, product_id):
    try:
        images_response = v3client.get(f'/catalog/products/{product_id}/images')
        return get_first_bigc_image_url_from_images_response(images_response)
    except Exception as e:
        print(f"SINGLE PRODUCT IMAGE LOOKUP FAILED for BigC ID {product_id}: {type(e).__name__}: {e}")
        return ""


def main(argv):
    queued_requests = []
    usdRate = getExchangeRate()

    v3client = bigcommerce.connection.OAuthConnection(
        client_id='gnw0dh7xzx97ck9pw4kwpthvefui412',
        store_hash='z84xkjcnbz',
        access_token='tqhvp7fmyqr438pewjwtcwi1vggxpky',
        api_path='/stores/{}/v3/{}'
    )

    def submitRequest(requestBody, isLast):
        batch_size = 50

        if requestBody != '':
            queued_requests.append({
                'batchId': len(queued_requests),
                'merchantId': '507522930',
                'method': 'insert',
                'product': requestBody,
                'updateMask': 'gtin,availability,brand,channel,condition,contentLanguage,customAttributes,description,id,imageLink,link,maxHandlingTime,minHandlingTime,mpn,offerId,price,productHeight,productLength,productWeight,productWidth,sellOnGoogleQuantity,targetCountry,title,customLabel0,itemGroupId,identifierExists,material'
            })

        if (len(queued_requests) >= batch_size) or (isLast is True and len(queued_requests) > 0):
            try:
                conn4 = get_db_connection()
            except mariadb.Error as e:
                print(f"Error connecting to MariaDB Platform: {e}")
                sys.exit(1)

            cur4 = conn4.cursor()
            service, config, _ = common.init(argv, __doc__)

            batch = {'entries': queued_requests}
            request = service.products().custombatch(body=batch)
            result = request.execute()

            if result.get('kind') == 'content#productsCustomBatchResponse':
                entries = result.get('entries', [])
                for entry in entries:
                    product = entry.get('product')
                    errors = entry.get('errors')

                    if product:
                        offer_id = product.get('offerId', '').strip()
                        print('sent ' + offer_id + ' to ' + product.get('targetCountry', 'unknown'))
                        cur4.execute(
                            "UPDATE product SET statusMerchantCenter='1' WHERE sku=?",
                            (offer_id,)
                        )
                        conn4.commit()
                    elif errors:
                        print('Errors for batch entry %d:' % entry.get('batchId', -1))
                        print(json.dumps(errors, sort_keys=True, indent=2, separators=(',', ': ')))
            else:
                print('There was an error. Response: %s' % result)

            queued_requests.clear()
            conn4.close()

    try:
        conn = get_db_connection()
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)

    totalSubmission = 0
    cur = conn.cursor()

    cur.execute("SELECT * FROM product WHERE statusMerchantCenter='2' AND custom_fields LIKE '%\"googlemcavail\"\\:true%'")

    productsAll = []
    count = 0
    for prod in cur:
        count += 1
        productsAll.append(createItem(prod))
    print(count)
    conn.close()

    counter = 0
    last = False

    for prod in productsAll:
        counter += 1
        if counter == count:
            last = True

        print('Handling ' + prod['sku'])

        custom_fields = safe_json_loads(prod['custom_fields'])
        visible_udf_data = safe_json_loads(prod['visible_udf_data'])
        upcs_values = safe_json_loads(prod['upcs_values'])

        # If parent, ignore. Only single products and variants need to go up.
        if custom_fields.get('primaryselector', '') != prod['sku']:
            productGeneral = {
                'offerId': prod['sku'],
                'title': prod['product_name'],
                'description': custom_fields.get('prodhtml', ''),
                'contentLanguage': 'en',
                'channel': 'online',
                'availability': 'in stock',
                'condition': 'new',
                'sellOnGoogleQuantity': 11,
                'minHandlingTime': int(custom_fields.get('RegProdDel', 1)),
                'maxHandlingTime': int(custom_fields.get('prodelrange', 0)) + int(custom_fields.get('RegProdDel', 1)),
                'productWeight': {'value': str(prod['weight']), 'unit': 'lb'}
            }

            if 'mpn' in custom_fields:
                if custom_fields.get('mpn', '') != '':
                    productGeneral['mpn'] = custom_fields.get('mpn')
                elif visible_udf_data.get('brandname', '').lower() == "safety media":
                    productGeneral['mpn'] = prod['sku']
            elif visible_udf_data.get('brandname', '').lower() == "safety media":
                productGeneral['mpn'] = prod['sku']

            if visible_udf_data.get('brandname', '') != "":
                productGeneral['brand'] = visible_udf_data.get('brandname')

            if custom_fields.get('googlepromoid', '') != "":
                productGeneral['customLabel0'] = custom_fields.get('googlepromoid')
            else:
                productGeneral['customLabel0'] = 'no_label'

            # Handle size
            sizeSet = False
            issueWithValues = False
            size_value = custom_fields.get('size', '')

            if (
                size_value != "" and
                size_value[0].isdigit() and
                (('"W' in size_value) or ('mmW' in size_value)) and
                (('"H' in size_value) or ('mmH' in size_value))
            ):
                a = 0
                dimensions = size_value.split('x')
                for dimension in dimensions:
                    dimUnit = ''
                    dim = ''
                    if '"' in dimension:
                        dim = dimension.split('"')[0].strip()
                        dimUnit = 'in'
                    elif 'mm' in dimension:
                        dim = dimension.split('mm')[0].strip()
                        dimUnit = 'mm'

                    if dim == '' or not isValidNumber(dim):
                        issueWithValues = True

                    match a:
                        case 0:
                            if dimUnit == 'in':
                                productGeneral['productWidth'] = {'value': dim, 'unit': 'in'}
                            elif dimUnit == 'mm':
                                productGeneral['productWidth'] = {'value': int(dim) / 10.0, 'unit': 'cm'}
                            sizeSet = True
                        case 1:
                            if dimUnit == 'in':
                                productGeneral['productHeight'] = {'value': dim, 'unit': 'in'}
                            elif dimUnit == 'mm':
                                productGeneral['productHeight'] = {'value': int(dim) / 10.0, 'unit': 'cm'}
                        case 2:
                            if dimUnit == 'in':
                                productGeneral['productLength'] = {'value': dim, 'unit': 'in'}
                            elif dimUnit == 'mm':
                                productGeneral['productLength'] = {'value': int(dim) / 10.0, 'unit': 'cm'}
                    a += 1

                if a == 2:
                    depth_value = custom_fields.get('depth')
                    if depth_value is not None and depth_value != '':
                        productGeneral['productLength'] = {'value': depth_value, 'unit': 'in'}

            if issueWithValues:
                sizeSet = False

            if (sizeSet is False) and (custom_fields.get('primaryselector', '') != ""):
                if 'productWidth' in productGeneral:
                    del productGeneral['productWidth']
                if 'productHeight' in productGeneral:
                    del productGeneral['productHeight']
                if 'productLength' in productGeneral:
                    del productGeneral['productLength']

                try:
                    conn2 = get_db_connection()
                except mariadb.Error as e:
                    print(f"Error connecting to MariaDB Platform: {e}")
                    sys.exit(1)

                cur2 = conn2.cursor()
                cur2.execute(
                    "SELECT custom_fields FROM product WHERE sku=?",
                    (prod['parent_product_code'],)
                )

                for parent in cur2:
                    parent_cf = safe_json_loads(parent[0])
                    selectorref = parent_cf.get('selectorref', '').lower()

                    if 'size' in selectorref:
                        position = selectorref[0:selectorref.index('size')].count(',') + 1
                        dropdownDimensions = custom_fields.get('ref' + str(position), '')

                        if ('w' in dropdownDimensions.lower()) and ('h' in dropdownDimensions.lower()):
                            dimCount = 0
                            dimensions = dropdownDimensions.split('x')

                            for dimension in dimensions:
                                amount = None
                                unit = None

                                if '"' in dimension:
                                    amount = float(dimension.split('"')[0].strip())
                                    unit = 'in'
                                elif 'mm' in dimension.lower():
                                    amount = float(dimension.lower().split('mm')[0].strip()) / 10
                                    unit = 'cm'
                                elif 'cm' in dimension.lower():
                                    amount = float(dimension.lower().split('cm')[0].strip())
                                    unit = 'cm'

                                if amount is not None and unit is not None:
                                    match dimCount:
                                        case 0:
                                            productGeneral['productWidth'] = {'value': amount, 'unit': unit}
                                            sizeSet = True
                                        case 1:
                                            productGeneral['productHeight'] = {'value': amount, 'unit': unit}
                                        case 2:
                                            productGeneral['productLength'] = {'value': amount, 'unit': unit}
                                dimCount += 1
                conn2.close()

            if sizeSet is False:
                productGeneral['productWidth'] = {'value': custom_fields.get('Width', ''), 'unit': 'in'}
                productGeneral['productHeight'] = {'value': custom_fields.get('Height', ''), 'unit': 'in'}
                productGeneral['productLength'] = {'value': custom_fields.get('depth', ''), 'unit': 'in'}
                sizeSet = True

            # Handle customAttributes
            totalCustoms = []
            if custom_fields.get('includes', '') != "":
                totalCustoms.append(custom_fields.get('includes'))
            if custom_fields.get('selfadhesivesticker') is True:
                totalCustoms.append('Self-Adhesive Sticker')
            if custom_fields.get('2sidedtape') is True:
                totalCustoms.append('2 Sided Tape')
            if custom_fields.get('slotholes') is True:
                totalCustoms.append('Slot Holes')
            if custom_fields.get('holes') is True:
                totalCustoms.append('Holes')
            if custom_fields.get('screws') is True:
                totalCustoms.append('Screws')
            if len(totalCustoms) != 0:
                productGeneral['customAttributes'] = [
                    {"name": "Includes", "value": ", ".join(totalCustoms)}
                ]

            # Handle material
            if custom_fields.get('material', '') != "":
                productGeneral['material'] = custom_fields.get('material')
            else:
                if custom_fields.get('primaryselector', '') != "":
                    try:
                        conn2 = get_db_connection()
                    except mariadb.Error as e:
                        print(f"Error connecting to MariaDB Platform: {e}")
                        sys.exit(1)

                    cur2 = conn2.cursor()
                    cur2.execute(
                        "SELECT custom_fields FROM product WHERE sku=?",
                        (prod['parent_product_code'],)
                    )

                    for parent in cur2:
                        parent_cf = safe_json_loads(parent[0])
                        selectorref = parent_cf.get('selectorref', '').lower()

                        if 'material' in selectorref:
                            position = selectorref[0:selectorref.index('material')].count(',') + 1
                            productGeneral['material'] = custom_fields.get('ref' + str(position), '')
                    conn2.close()

            # Variant
            if prod['variant_id'] is not None:
                try:
                    try:
                        conn3 = get_db_connection()
                    except mariadb.Error as e:
                        print(f"Error connecting to MariaDB Platform: {e}")
                        sys.exit(1)

                    cur2 = conn3.cursor()
                    cur2.execute(
                        "SELECT custom_fields, bigc_product_id FROM product WHERE sku=?",
                        (prod['parent_product_code'],)
                    )

                    variantURLCA = ''
                    variantURLUS = ''
                    parentBigcID = None

                    for parent in cur2:
                        parent_cf = safe_json_loads(parent[0])
                        variantURLCA = 'https://safetymedia.com/' + fixURL(parent_cf.get('seourl', '')).lower() + '?sku=' + prod['sku']
                        variantURLUS = 'https://safetymedia.com/' + fixURL(parent_cf.get('seourl', '')).lower() + '?sku=' + prod['sku']
                        parentBigcID = parent[1]

                    conn3.close()

                    if parentBigcID is None:
                        raise Exception("Parent BigCommerce product ID not found")

                    variantBigCInfo = v3client.get(f'/catalog/products/{parentBigcID}/variants/{prod["variant_id"]}')
                    variant_image = variantBigCInfo.get('data', {}).get('image_url', '')
                    parent_image = get_parent_product_image(v3client, parentBigcID)
                    final_variant_image = variant_image if variant_image else parent_image

                    variantCA = copy(productGeneral)
                    variantCA['id'] = 'online:EN:CA:' + prod['sku']
                    variantCA['link'] = variantURLCA
                    variantCA['imageLink'] = final_variant_image
                    variantCA['targetCountry'] = 'CA'
                    variantCA['price'] = {'value': prod['price'], 'currency': 'CAD'}
                    variantCA['itemGroupId'] = prod['parent_product_code']
                    variantCA['customLabel0'] = productGeneral['customLabel0'] + '-CA'

                    if upcs_values.get('upc', '') != '':
                        variantCA['gtin'] = upcs_values.get('upc')
                        variantCA['identifierExists'] = True
                    elif custom_fields.get('isbn', '') != '':
                        variantCA['gtin'] = custom_fields.get('isbn')
                        variantCA['identifierExists'] = True

                    if not variantCA.get('imageLink'):
                        print(f"{prod['sku']} CA skipped: no imageLink available")
                    else:
                        submitRequest(variantCA, False)
                        totalSubmission += 1

                    variantUS = copy(variantCA)
                    variantUS['id'] = 'online:EN:US:' + prod['sku']
                    variantUS['link'] = variantURLUS
                    variantUS['targetCountry'] = 'US'

                    if usdRate == 0:
                        variantUS['price'] = {'value': round(float(prod['price']) / 1.25, 2), 'currency': 'USD'}
                    else:
                        variantUS['price'] = {'value': round(float(prod['price']) / float(usdRate), 2), 'currency': 'USD'}

                    variantUS['customLabel0'] = productGeneral['customLabel0'] + '-US'

                    if custom_fields.get('googlemcexcludeus', False) is False:
                        if not variantUS.get('imageLink'):
                            print(f"{prod['sku']} US skipped: no imageLink available")
                        else:
                            submitRequest(variantUS, last)
                            totalSubmission += 1

                except Exception as e:
                    print(f"{prod['sku']} variant flow failed: {type(e).__name__}: {e}")

            # Single product
            else:
                if prod['parent_product_code'] != "":
                    print(prod['sku'] + " Variant should be on BigC but isn't")
                else:
                    try:
                        singleCA = copy(productGeneral)
                        singleCA['id'] = 'online:EN:CA:' + prod['sku']
                        singleCA['link'] = 'https://safetymedia.com/' + prod['sku'] + '/' + fixURL(custom_fields.get('seourl', '')).lower()

                        single_image = get_single_product_image(v3client, prod['bigc_product_id'])
                        singleCA['imageLink'] = single_image
                        singleCA['targetCountry'] = 'CA'
                        singleCA['price'] = {'value': prod['price'], 'currency': 'CAD'}
                        singleCA['customLabel0'] = productGeneral['customLabel0'] + '-CA'

                        if upcs_values.get('upc', '') != '':
                            singleCA['gtin'] = upcs_values.get('upc')
                            singleCA['identifierExists'] = True
                        elif custom_fields.get('isbn', '') != '':
                            singleCA['gtin'] = custom_fields.get('isbn')
                            singleCA['identifierExists'] = True

                        if not singleCA.get('imageLink'):
                            print(f"{prod['sku']} CA skipped: no imageLink available")
                        else:
                            submitRequest(singleCA, False)
                            totalSubmission += 1

                        singleUS = copy(singleCA)
                        singleUS['id'] = 'online:EN:US:' + prod['sku']
                        singleUS['link'] = 'https://safetymedia.com/' + prod['sku'] + '/' + fixURL(custom_fields.get('seourl', '')).lower()
                        singleUS['targetCountry'] = 'US'

                        if usdRate == 0:
                            singleUS['price'] = {'value': round(float(prod['price']) / 1.25, 2), 'currency': 'USD'}
                        else:
                            singleUS['price'] = {'value': round(float(prod['price']) / float(usdRate), 2), 'currency': 'USD'}

                        singleUS['customLabel0'] = productGeneral['customLabel0'] + '-US'

                        if custom_fields.get('googlemcexcludeus', False) is False:
                            if not singleUS.get('imageLink'):
                                print(f"{prod['sku']} US skipped: no imageLink available")
                            else:
                                submitRequest(singleUS, last)
                                totalSubmission += 1

                    except Exception as e:
                        print(f"{prod['sku']} single-product flow failed: {type(e).__name__}: {e}")

    submitRequest('', True)
    print('Total submissions: ' + str(totalSubmission))
    quit()


if __name__ == '__main__':
    main(sys.argv)