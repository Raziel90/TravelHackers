from amadeus import Client, ResponseError
from amadeus import Location
from json import loads
import pandas as pd
import logging


class API_Caller():
    def __init__(self, api_key, api_secret):
        self.client_id = api_key'GhuQAmG2XMQUUNHSV1mj5P4A4xgrIBqe'
        self.client_secret = api_secret'LEOnmdMime2btHXN'
        self.logger = logging.getLogger('API_LOG')
        self.logger.setLevel(logging.INFO)
            


class TravelShuffleAPI(API_Caller):
    def __init__(self, api_key, api_secret):
        super().__init__(api_key, api_secret)

        self.amadeus = Client(
            client_id=self.client_id, 
            client_secret=self.client_secret,
            log_level='debug')

    def getAirportsbyLocationKeyword(self, keyword):
        response = amadeus.reference_data.locations.get(
            keyword='LONDON',
            subType=Location.AIRPORT
            ).data

        airport_by_key = DataFrame(response)
        result = airport_by_key[['detailedName', 'iatacode', 'id', 'name', 'timeZoneOffset']]
        locations = airport_by_key['geoCode'].apply(pd.Series)
        score = airport_by_key['analytics'].apply(pd.Series)['travelers']
        return {'airports_from_keyword': pd.concat([result, locations, score], axis = 1).to_dict()}

    def getoriginsfromgeo(self, lat, long):
        raise NotImplementedError

    def getAvailableDestinations(self, origin_keys):
        if type(origin_keys) is not list:
            origin_keys = [origins]
        destination_per_origins = []
        for origin in origin_keys:
            response += amadeus.shopping.flight_destinations.get(origin=origin)
            available_trips = flight_df[['origin', 'destination' , 'departureDate', 'returnDate']]
            price = flight_df.price.apply(pd.Series)
            dates_offers_api_links = flight_df.links.apply(pd.Series)
            destination_per_origin = pd.concat([available_trips, price, dates_offers_api_links], axis = 1)
            destination_per_origins.append(destination_per_origin)

        return {'destination_tab': pd.concat(destination_per_origins, axis=0).to_dict()}

    def getHotelandOffer(self, locations):
        hotelsonDEST = []
        for dest in locations:
            hotelsonDEST += amadeus.shopping.hotel_offers.get(cityCode = dest).data

        # Convert response to pandas
        hotels_df = pd.DataFrame(hotelsonDEST)
        # CLEAN HOTEL INFO HERE
        hotel_info = hotels_df.hotel.apply(pd.Series)
        hotel_basic = hotel_info[['hotelId', 'dupeId', 'chainCode', 'cityCode', 'name', 'rating', 'type', 'latitude', 'longitude', 'media', 'amenities']]
        hotel_address_info = hotel_info.address.apply(pd.Series)
        hotel_desc = hotel_info.description.apply(pd.Series)[['lang', 'text']].fillna('').add_suffix('_description')
        # CLEAN OFFER INFO HERE
        offer_tab = pd.concat([hotels_df, hotel_basic[['hotelId', 'dupeId', 'chainCode', 'cityCode']]],axis=1).drop(['hotel'], axis=1)
        offer_tab = offer_tab[['hotelId', 'dupeId', 'chainCode', 'cityCode', 'available', 'self', 'offers']]
        # THERE MAY BE MORE THAN ONE OFFER PER HOTEL
        offer_info = offer_tab.offers.apply(pd.Series) \
            .merge(offer_tab[['hotelId', 'dupeId', 'chainCode', 'cityCode', 'available', 'self', 'offers']], left_index = True, right_index = True) \
            .drop(['offers'], axis=1) \
            .melt(id_vars = ['hotelId', 'dupeId', 'chainCode', 'cityCode', 'available', 'self'], value_name = "offer", var_name='offer_n')
        offer_detail = offer_info.offer.apply(pd.Series)
        offer_commission = offer_detail.commission.apply(pd.Series)[['percentage', 'amount']].add_prefix('commission_')
        offer_guests = offer_detail.guests.apply(pd.Series).add_prefix('guest_available__')
        offer_pricing = offer_detail.price.apply(pd.Series)[['currency', 'total']].add_prefix('pricing_')
        # CLEAN ROOM INFO HERE
        room_detail = offer_detail.room.apply(pd.Series)
        room_desc = room_detail.description.apply(pd.Series).add_suffix('_description')
        room_cat = room_detail.typeEstimated.apply(pd.Series)
        room_tab = pd.concat([room_cat, room_detail, room_desc],axis=1).drop(['description', 'typeEstimated'],axis=1)
        offer_detail_tab = pd.concat([
            offer_detail.drop(['commission', 'guests', 'policies', 'price', 'room', 'rateFamilyEstimated', 'boardType'], axis=1).add_prefix('offer_'),
            offer_guests, offer_pricing, offer_commission, room_tab], axis=1)

        # COMBINE IN 2 PANDAS
        offer_total_tab = pd.concat([offer_info, offer_detail_tab], axis=1).drop(['offer'], axis=1)
        hotel_total_tab = pd.concat([hotel_basic, hotel_address_info, hotel_desc], axis=1)
        return {'hotel_tab': hotel_total_tab.to_dict(), 'offer_tab': offer_total_tab}



if __name__ == "__main__":
    pass
    try:
        api_key = 'GhuQAmG2XMQUUNHSV1mj5P4A4xgrIBqe'
        api_secret = 'LEOnmdMime2btHXN'
        TravelShuffleAPI(api_key = 'GhuQAmG2XMQUUNHSV1mj5P4A4xgrIBqe',
            api_secret = 'LEOnmdMime2btHXN'
            )
    except ResponseError as error:
        print(error)    