import requests
import json
import logging


API_ENDPOINTS = None

def load_endpoints():
    global API_ENDPOINTS
    with open('api_endpoints.json', 'r') as f:
        API_ENDPOINTS = json.load(f)



def get_id_tenders_from_nif(nif):
    '''
        Returns:
            <list> of str or None if not found
    '''

    assert type(nif) is str, "nif provided must be str"

    request_url = API_ENDPOINTS['complete_company_info_endpoint']
    res_request = request_url + nif
    response = requests.get(res_request)
    if response.status_code != 200:
        logging.warning(f'Unable to get id_tender from NIF:{nif}. API status code: {response.status_code} ')
        return None
    
    id_urls =  response.json()['id_tender']
    id_tenders = []

    if type(id_urls) is  str:
        id_tenders = [id_urls.split('/')[-1]]
    else:
        id_tenders = [ i.split('/')[-1] for i in id_urls]

    return id_tenders





def get_procurement_place_id_from_id_tender(id_tender):
    '''

        To look for and id we:

            1. First look in place_menores endpoint
            2. Look in place endpoint (outsiders + insiders)
    
    
        Returns id (ntpXXXXX) or None if sm wrong
    '''

    assert type(id_tender) is str, "id_tender provided must be str"

    # Menores endpoint
    request_url = API_ENDPOINTS['procurement_minors_info_endpoint']
    res_request = request_url + id_tender
    response = requests.get(res_request)


    if response.status_code == 200 and '_id' in response.json().keys() :
        # a minor
        place_id =  response.json()['_id']
        assert type(place_id) is str, 'Place internal id is not type str'
        return place_id
    
    else:

        # insider or outsider endpoint
        request_url = API_ENDPOINTS['procurement_insiders_and_outsiders_info_endpoint']
        res_request = request_url + id_tender
        response2 = requests.get(res_request)

        if response2.status_code == 200 and '_id' in response2.json().keys():
            place_id =  response2.json()['_id']
            assert type(place_id) is str, 'Place internal id is not type str'
            return place_id
        else:
            # Err
            logging.warning(f'Unable to get procurement_place_id from id_tender:{id_tender}. API status code: {response.status_code} ')
            return None


    


def get_procurements_place_ids_from_ute_nif(ute_nif):

    ''' Given an UTE nif return all the licitations and its pdfs docs.
    
        Returns:

            list: list of interal PLACE ids (nptXXXX). One per procurement
    '''


    load_endpoints()
    assert API_ENDPOINTS is not None, "Error, cannot load API endpoints from api_endpoints.json config file."

 

    id_tenders = get_id_tenders_from_nif(ute_nif)

    internal_ids = []
    for id_tender in id_tenders:
        place_id = get_procurement_place_id_from_id_tender(id_tender)
        internal_ids.append(place_id)
        
    return internal_ids






'''
def main():


    # Debug
    #request_nif = 'b86653482'
    request_nif = '23210609k'
    #request_id_tender

    load_endpoints()
    assert API_ENDPOINTS is not None, "Error, cannot load API endpoints from api_endpoints.json config file."



    id_tenders = get_id_tenders_from_nif(request_nif)
    #print(f'Los id_tender son {id_tenders} with type {type(id_tenders)}')


    for id_tender in id_tenders:
        place_id = get_procurement_place_id_from_id_tender(id_tender)
        print(f'\t id_tender: {id_tender} place internal id is {place_id}')

'''



#if __name__ == "__main__":
#    main()