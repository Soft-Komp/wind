"""
app/api/comments.py
===================
Router komentarzy — System Windykacja.

Operacje CRUD komentarzy są dostępne jako pod-zasoby dłużników:
    GET    /debtors/{id}/comments                    — lista komentarzy
    POST   /debtors/{id}/comments                    — dodaj komentarz
    PUT    /debtors/{id}/comments/{comment_id}        — edytuj komentarz
    DELETE /debtors/{id}/comments/{comment_id}        — inicjuj usunięcie (krok 1)
    DELETE /debtors/{id}/comments/{comment_id}/confirm — potwierdź usunięcie (krok 2)

Ten plik rejestruje router w api/router.py (prefix /comments).
Rozszerzenia standalone (np. GET /comments/{id} bez debtor_id) dodawać tutaj.
"""

from fastapi import APIRouter

router = APIRouter()