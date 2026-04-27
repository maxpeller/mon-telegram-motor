"""
Suivi en mémoire de l'état des synchronisations d'historique.

Chaque compte a un statut { running, error, result } mis à jour par
_run_sync dans main.py. Cela permet à /sync/status/<account_id> de
retourner l'état courant sans bloquer la requête initiale.
"""
from typing import Any, Dict, Optional

_sync_status: Dict[str, Dict[str, Any]] = {}


def get_sync_status(account_id: str) -> Optional[Dict[str, Any]]:
    """Retourne le statut de synchronisation pour un compte, ou None si jamais lancé."""
    return _sync_status.get(account_id)


def set_sync_status(account_id: str, status: Dict[str, Any]) -> None:
    """Met à jour le statut de synchronisation pour un compte."""
    _sync_status[account_id] = status


def clear_sync_status(account_id: str) -> None:
    """Supprime le statut de synchronisation pour un compte."""
    _sync_status.pop(account_id, None)
