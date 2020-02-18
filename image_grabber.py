#!/usr/bin/env python3.5
# -*- coding: utf-8 -*-
"""
Created on Wed May 23 09:16:29 2019

@author: Anurag Sharma
"""

import paramiko

class ImageGrabber:
    
    def server_to_server(self,image_type,image_name):
        remote_file = '/home/amss/amss_images/{}_dlh/{}.gif'.format(image_type,image_name)
        local_file = '/home/sanand/radar/images/{}.gif'.format(image_name)
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname='139.59.16.54',username='*******',password='*********',port=22)
        ftp_client=ssh_client.open_sftp()
        ftp_client.get(remote_file,local_file)
        ftp_client.close()
        ssh_client.close()
    
    def server_to_local(self,image_type,image_name):
        remote_file = '/home/amss/amss_images/{}_dlh/{}.gif'.format(image_type,image_name)
        local_file = 'C:/Users/anurag/Documents/Python Scripts/RadarImages/server/images/{}.gif'.format(image_name)
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname='139.59.16.54',username='*****',password='*********',port=22)
        ftp_client=ssh_client.open_sftp()
        ftp_client.get(remote_file,local_file)
        ftp_client.close()
        ssh_client.close()
        
        
        
