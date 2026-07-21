# -*- coding: utf-8 -*-
"""
Sprawdza, czy haslo zostalo poprawnie zinterpretowane jako UTF-8 —
NIE wypisuje samego hasla, tylko jego dlugosc w znakach vs bajtach
oraz reprezentacje hex (ktora TY mozesz porownac z tym, co wpisales,
ja nigdy tego nie zobacze).
"""
import getpass

password = getpass.getpass("Podaj haslo do klucza: ")

print(f"Dlugosc w znakach (Python str): {len(password)}")
print(f"Dlugosc w bajtach (UTF-8):      {len(password.encode('utf-8'))}")
print(f"Reprezentacja hex (UTF-8):      {password.encode('utf-8').hex()}")
print()
print("Jesli haslo skladalo sie WYLACZNIE z liter ASCII/cyfr/podstawowych")
print("symboli — dlugosc w znakach i w bajtach POWINNA byc identyczna.")
print("Jesli sa rozne — to znak(i) specjalne zostaly zinterpretowane inaczej")
print("niz oczekiwano (mozliwy problem kodowania PowerShell/konsoli).")
print()
print("Porownaj hex powyzej z tym, co faktycznie wpisales (Ty wiesz, jak")
print("wyglada Twoje haslo znak po znaku w ASCII/UTF-8) — to Twoja")
print("samodzielna weryfikacja, ja nie widze samego hasla.")