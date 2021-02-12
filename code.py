#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 24 15:00:34 2020

@author: steve
"""
from mrjob.job import MRJob
from mrjob.step import MRStep
import time


def is_in_0to1_intervall(a):
    return a<=1 and a>0
   
def text_readline(line):
    return line.split("\t")

    
class Mapper(MRJob):
    m = 1/75879
    
    is_float = 'ok'
    try:
        c = float(input("Entrer la valeur du coéfficient c : "))
        print(end = '\n')
    except Exception:
        print("Veuillez entrer un réel de l'intervalle ]0, 1] .")
        print(end = '\n')
        is_float = None
    while is_float == None:
        is_float = 'ok'
        try:
            print(end = '\n')
            c = float(input("Entrer la valeur du coéfficient c : "))
            print(end = '\n')
        except Exception:
            print("Veuillez entrer un réel de l'intervalle ]0, 1] .")
            print(end = '\n')
            is_float = None
    
            
    while  not is_in_0to1_intervall(c):
        print(end = '\n')
        print("c doit vivre dans l'intervalle ]0, 1] .")
        
        is_float = 'ok'
        try:
            c = float(input("Entrer la valeur du coéfficient c : "))
            print(end = '\n')
        except Exception:
            print("Veuillez entrer un réel de l'intervalle ]0, 1] .")
            print(end = '\n')
            is_float = None
        while is_float == None:
            is_float = 'ok'
            try:
                print(end = '\n')
                c = float(input("Entrer la valeur du coéfficient c : "))
                print(end = '\n')
            except Exception:
                print("Veuillez entrer un réel de l'intervalle ]0, 1] .")
                print(end = '\n')
                is_float = None
 
    
    
        
    is_interger = 'ok'
    try:
        n = int(input("Entrer le nombre d'itérations n  :  "))
        print(end = '\n')
    except Exception:
        print("Veuillez entrer un nombre entier s'il vous plaît, de préférence inférieur à 10  :  ")
        print(end = '\n')
        is_interger = None
    while is_interger == None:
        is_interger = 'ok'
        try:
            print(end = '\n')
            n = int(input("Entrer le nombre d'itérations n  :  "))
            print(end = '\n')
        except Exception:
            print("Veuillez entrer un nombre entier s'il vous plaît, de préférence inférieur à 10  :  ")
            print(end = '\n')
            is_interger = None
    
    print("Super !")
    print(end = "\n")
    print("Voici les affichages possibles :")
    print("a.    page    pageRank : ***      Pages Citées: [ *** ]")
    print("b.    page    pageRank = ***      ")
    print(end = "\n")
    affichage = input("Enter a pour l'affichage a et b pour l'affichage b (a/b):   ")
    print(end = "\n")
    pages_acquitees = list()
    def mapper(self, _, line):
        cell = text_readline(line)
        c1,c2 = int(cell[0]),int(cell[1])
        yield c1,c2
        
    def reducer(self,page, voisins):
        
        '''
            J'appelle pages_acquitees les pages qui ont déjà reçu la masse initiale de 1/n
            Toutes les pages sources sont dans un premiers temps concernées. Ensuite , pour
            chaque page cible, je vérifie si celle-ci est déjà page source, auquel cas je 
            lui ajoute uniquement la masse qu'il gagne en étant citée car toutes les pages 
            sont déja recompensées de 1/n. Si elle n'est pas  dans pages_acquitées, je lui 
            ajoute d'abord 1/n puis la masse qu'elle gagne en étant citée et je l'ajoute dans
            pages_acquitees.'''
        Mapper.pages_acquitees.append(page)
        
        p = {'pageRank': Mapper.m, 'AdjencyList': voisins}
        yield page, p
        
    def mapper1(self,page,voisins):
        pageRank_courant = Mapper.c*voisins['pageRank']
        yield page, {'pageRank' :pageRank_courant, 'AdjencyList': voisins['AdjencyList']}
        p = (1-Mapper.c)*voisins['pageRank']/len(list(voisins['AdjencyList']))
        
        for v in voisins['AdjencyList']:
            if v in  Mapper.pages_acquitees:
                yield v, {'pageRank': p, 'AdjencyList': list()}
            else:
                Mapper.pages_acquitees.append(v)
                
                yield v, {'pageRank': p+Mapper.m, 'AdjencyList': list()}
                
        
    def reducer1(self,page, description):
        page_ranks = 0
        voisins = list()
        for d in description:
            page_ranks += d['pageRank']
            voisins = voisins + d['AdjencyList']
        yield page, {'pageRank':page_ranks, 'AdjencyList': voisins }
        
    def mapper2(self, page, description):
        if description['AdjencyList'] != list():
            p = (1-Mapper.c)*description['pageRank']/len(list(description['AdjencyList']))
            description['pageRank'] *= Mapper.c
            yield page, description
            
            for v in description['AdjencyList']:
                yield v, {'pageRank':p, 'AdjencyList': list()}
        else:
            description['pageRank'] = Mapper.c*Mapper.n
            yield page, description
                
    def reducer2(self,page, description):
        
        page_ranks = 0
        voisins = list()
        for d in description:
            page_ranks += d['pageRank']
            voisins = voisins + d['AdjencyList']
        yield page, {'pageRank':page_ranks , 'AdjencyList': voisins }
    
    def mapper_final(self,page, description):
        yield page, f"pageRank = {description['pageRank']}"
                              
    def steps(self):
        solutions = [MRStep(mapper=self.mapper,reducer=self.reducer)]
        ''' Ceci correspond au pagerank initial(n = 0) , celui par défaut dans lequel 
        la masse est identique pour toutes pages et vaut 1/n .
        J'ai paramétré de façon que toute valeur de n inférieure ou égale à 0 rentrée en ligne
        de commande corresponde au pageRank initial 1/n '''
        
        if Mapper.n == 1:
            solutions.append(MRStep(mapper=self.mapper1,reducer=self.reducer1))
        if Mapper.n > 1:
            solutions.append(MRStep(mapper=self.mapper1,reducer=self.reducer1))
            for i in range(Mapper.n-1):
                solutions.append(MRStep(mapper=self.mapper2,reducer=self.reducer2))
        if Mapper.affichage in 'Bb':
            solutions.append(MRStep(mapper=self.mapper_final))
        return solutions


if __name__ == '__main__':
    t = time.time()
    Mapper.run()
    print(end = "\n")
    print("Le programme a tourné pendant ",time.time() - t," secondes")
    print(end = "\n")