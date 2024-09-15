from django.shortcuts import render

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status


class MovioWorkerHealthcheck(APIView):
    '''Healthcheck API for Movio-Worker-Service'''
    
    def get(self, request):
        return Response({"status": "OK"}, status=status.HTTP_200_OK)