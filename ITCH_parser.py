import gzip
import struct
import pandas as pd
import numpy as np
import os
from datetime import timedelta

class ITCH_trade:
    def __init__(self, ITCH_data_path):
        """
        Initiate ITCH_trade object
        
        Parameter: 
        ITCH_data_path(string): Path of NASDAQ ITCH5.0 data file
        """
        self.ITCH_data_path = ITCH_data_path
        self.trade_df = None
        self.cross_trade_df = None
        
    def trade_message(self, msg):
        """
        Process NASDAQ ITCH5.0 Non-Cross Trade Message (Message type: "P")
        
        Parameter: 
        msg(binary number): A NASDAQ ITCH5.0 Non-Cross Trade Message (Message type: "P")
        
        Return: 
        Processed Non-Cross Trade Message with information saved in a list 
        [trade time, hour value of trade time + 1, buy/sell side("B", "S"), number of shares, stock code, trade price]
        """
        msg = struct.unpack('>4s6sQcI8cIQ',msg) # Unpack msg
        # Add 2 additional bytes for timestamp
        msg = struct.pack('>4s2s6sQsI8sIQ', msg[0], b'\x00\x00', msg[1], msg[2], \
                          msg[3],msg[4],b''.join(list(msg[5:13])),msg[13], msg[14]) 
        record = list(struct.unpack('>HHQQsI8sIQ', msg))
        time_str = '{0}'.format(timedelta(seconds = record[2] / 1e9)).split('.')[0] # Convert timestamp format to HH:MM:SS
        end_time_hour = int(time_str.split(':')[0]) + 1 # One-hour period end time of timestamp(e.g. 04:30:35 -> 5)
        record = [time_str, end_time_hour, record[4].decode('ascii'), record[5], \
                  record[6].decode('ascii').strip(), round(record[7]/10000, 3)] # Save information in a list
        return record

    def cross_trade_message(self, msg):
        """
        Process NASDAQ ITCH5.0 Cross Trade Message (Message type: "Q")
        
        Parameter- 
        msg(binary number): A NASDAQ ITCH5.0 Cross Trade Message (Message type: "Q")
        
        Return- 
        record(list):Processed Cross Trade Message with information saved in a list   
        [trade time, hour value of trade time + 1, cross type("O", "C", "H"), number of shares, stock code, trade price]
        """
        msg = struct.unpack('>4s6sQ8cIQc',msg) # Unpack msg
        # Add 2 additional bytes for timestamp
        msg = struct.pack('>4s2s6sQ8sIQs', msg[0], b'\x00\x00', msg[1], msg[2], \
                              b''.join(list(msg[3:11])), msg[11], msg[12], msg[13]);
        record = list(struct.unpack('>HHQQ8sIQs',msg))
        time_str = '{0}'.format(timedelta(seconds = record[2] / 1e9)).split('.')[0] # Convert timestamp format to HH:MM:SS
        end_time_hour = int(time_str.split(':')[0]) + 1 # One-hour period end time of timestamp(e.g. 04:30:35 -> 5)
        record = [time_str, end_time_hour, record[7].decode('ascii'), record[3], \
                  record[4].decode('ascii').strip(), round(record[5]/10000, 3)] # Save information in a list
        return record

    def get_trade_data(self):
        """
        Read through ITCH5.0 data and save non-cross trade information and cross trade information into two dataframes
        Return- 
        trade_df(DataFrame): dataframe of non-cross trade information
        cross_trade_df(DataFrame): dataframe of cross trade information
        """
        trade_list = [] # Create an empty list for storing non-cross trade information
        cross_trade_list = [] # Create an empty list for storing cross trade information
        bin_data = gzip.open(os.path.join(self.ITCH_data_path), 'rb') # Load data
        bin_data.read(2) # Pass the initial 2 control characters
        msg_type = bin_data.read(1) # Read the first message type
        # Loop through all messages
        while msg_type:
            assert(msg_type in [b'S', b'R', b'H', b'Y', b'L', b'V', b'W', b'K', b'J', b'h', b'A', \
                                b'F', b'E', b'C', b'X', b'D', b'U', b'P', b'Q', b'B', b'I']) # Insure msg_type is valid
            if msg_type == b'S':
                bin_data.read(13)
            elif msg_type == b'R':
                bin_data.read(40)
            elif msg_type == b'H':
                bin_data.read(26)
            elif msg_type == b'Y':
                bin_data.read(21)
            elif msg_type == b'L':
                bin_data.read(27)
            elif msg_type == b'V':
                bin_data.read(36)
            elif msg_type == b'W':
                bin_data.read(13)
            elif msg_type == b'K':
                bin_data.read(29)
            elif msg_type == b'J':
                bin_data.read(36)
            elif msg_type == b'h':
                bin_data.read(22)
            elif msg_type == b'A':
                bin_data.read(37)
            elif msg_type == b'F':
                bin_data.read(41)
            elif msg_type == b'E':
                bin_data.read(32)
            elif msg_type == b'C':
                bin_data.read(37)
            elif msg_type == b'X':
                bin_data.read(24)
            elif msg_type == b'D':
                bin_data.read(20)
            elif msg_type == b'U':
                bin_data.read(36)
            elif msg_type == b'P': # Process and store information of non-cross trades
                msg = bin_data.read(43)
                record = self.trade_message(msg)
                trade_list.append(record)     
                bin_data.read(2)
            elif msg_type == b'Q': # Process and store information of cross trades
                msg = bin_data.read(39)
                record = self.cross_trade_message(msg)
                cross_trade_list.append(record)     
                bin_data.read(2)
            elif msg_type == b'B':
                bin_data.read(20)
            elif msg_type == b'I':
                bin_data.read(51)
            msg_type = bin_data.read(1)
        # Convert list to DataFrame
        trade_df = pd.DataFrame(trade_list, columns = ['time', 'end_time_hour', 'type', 'shares', 'stock', 'price'])
        cross_trade_df = pd.DataFrame(cross_trade_list, columns = ['time', 'end_time_hour', 'type', 'shares', 'stock', 'price'])
        self.trade_df = trade_df
        self.cross_trade_df = cross_trade_df
        return self.trade_df, self.cross_trade_df
    
    def calculate_VWAP(self, trade_df):
        """
        Given a DataFrame of trade information, calculate hourly vwap and vwap for that trading day until the end 
        of each trading hour
        
        Parameter- 
        trade_df(DataFrame): dataframe of NASDAQ ITCH5.0 trade information with columns time, end_time_hour, type, 
        shares, stock, price
        
        Return-
        trade_df(DataFrame): dataframe including hourly vwap and vwap for that trading day until the end of each 
        trading hour for each stock 
        """
        trade_df['total_value'] = trade_df['price'] * trade_df['shares'] # Calculate dollar value for each trade
        # Calculate hourly vwap at the end of each trading hour for each stock
        trade_df = trade_df.groupby([trade_df['end_time_hour'], trade_df['stock']])['total_value', 'shares'].sum()
        trade_df['vwap'] = round(trade_df['total_value'] / trade_df['shares'], 3)
        trade_df = trade_df.reset_index()
        trade_df = trade_df.sort_values(['end_time_hour', 'stock'], ascending=[True, True])
        # Calculate total dollar value traded in that trading day at the end of each trading hour for each stock
        trade_df['cum_total_value'] = trade_df.groupby(trade_df['stock'])['total_value'].cumsum()
        # Calculate total shares traded in that trading day at the end of each trading hour for each stock
        trade_df['cum_shares'] = trade_df.groupby(trade_df['stock'])['shares'].cumsum()
        # Calculate vwap in that trading day at the end of each trading hour for each stock
        trade_df['cum_vwap'] = round(trade_df['cum_total_value'] / trade_df['cum_shares'], 3)
        return trade_df
       
    def get_VWAP_df(self, include_cross_trade = False):
        """
        Get dataframe with vwap
        
        Parameter- 
        include_cross_trade(boolean): whether to include cross trades in vwap calcualtion
        
        Return-
        VWAP_df(DataFrame): hourly vwap and vwap for that trading day until the end of each trading hour for each stock
        """
        # If NASDAQ ITCH5.0 data file hasn't been processed into trade information dataframes, process the raw data first 
        if (self.trade_df is None) or (self.cross_trade_df is None):
            self.trade_df, self.cross_trade_df = self.get_trade_data()
        # If user choose to include cross trade data, then combine non-cross trade and cross trade dataframe
        if include_cross_trade == True:
            total_trade_df = pd.concat([self.trade_df.copy(), self.cross_trade_df.copy()], ignore_index=True)
        else:
            total_trade_df = self.trade_df.copy()
        # Calculate vwap and cum_vwap
        VWAP_df = self.calculate_VWAP(total_trade_df)
        return VWAP_df
