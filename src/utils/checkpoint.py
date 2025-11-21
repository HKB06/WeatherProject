"""
Module de gestion des checkpoints pour l'ingestion résiliente
Permet de sauvegarder et reprendre l'ingestion en cas d'interruption
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
from src.utils.logger import get_logger

logger = get_logger(__name__)


class CheckpointManager:
    """Gestionnaire de checkpoints pour l'ingestion de données"""
    
    def __init__(self, checkpoint_dir: str = "checkpoints"):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(exist_ok=True, parents=True)
        logger.info(f"CheckpointManager initialisé avec le répertoire: {self.checkpoint_dir}")
    
    def save_checkpoint(self, job_name: str, state: Dict[str, Any]) -> None:
        """
        Sauvegarde un checkpoint
        
        Args:
            job_name: Nom du job (utilisé pour nommer le fichier)
            state: État à sauvegarder (dict)
        """
        try:
            checkpoint_file = self.checkpoint_dir / f"{job_name}.checkpoint.json"
            
            # Ajouter des métadonnées
            state['_checkpoint_metadata'] = {
                'saved_at': datetime.now().isoformat(),
                'job_name': job_name
            }
            
            # Sauvegarder
            with open(checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=2, default=str)
            
            logger.info(f"Checkpoint sauvegardé: {checkpoint_file}")
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde du checkpoint: {e}")
            raise
    
    def load_checkpoint(self, job_name: str) -> Optional[Dict[str, Any]]:
        """
        Charge un checkpoint existant
        
        Args:
            job_name: Nom du job
        
        Returns:
            État sauvegardé ou None si pas de checkpoint
        """
        try:
            checkpoint_file = self.checkpoint_dir / f"{job_name}.checkpoint.json"
            
            if not checkpoint_file.exists():
                logger.info(f"Aucun checkpoint trouvé pour {job_name}")
                return None
            
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                state = json.load(f)
            
            logger.info(f"Checkpoint chargé pour {job_name}")
            
            # Afficher les métadonnées
            if '_checkpoint_metadata' in state:
                metadata = state['_checkpoint_metadata']
                logger.info(f"Checkpoint créé le: {metadata.get('saved_at', 'Unknown')}")
            
            return state
            
        except Exception as e:
            logger.error(f"Erreur lors du chargement du checkpoint: {e}")
            return None
    
    def clear_checkpoint(self, job_name: str) -> None:
        """
        Supprime un checkpoint après succès complet
        
        Args:
            job_name: Nom du job
        """
        try:
            checkpoint_file = self.checkpoint_dir / f"{job_name}.checkpoint.json"
            
            if checkpoint_file.exists():
                checkpoint_file.unlink()
                logger.info(f"Checkpoint supprimé: {checkpoint_file}")
            else:
                logger.debug(f"Aucun checkpoint à supprimer pour {job_name}")
                
        except Exception as e:
            logger.error(f"Erreur lors de la suppression du checkpoint: {e}")
    
    def list_checkpoints(self) -> list:
        """
        Liste tous les checkpoints existants
        
        Returns:
            Liste des noms de jobs avec checkpoints
        """
        try:
            checkpoints = []
            for file in self.checkpoint_dir.glob("*.checkpoint.json"):
                job_name = file.stem.replace('.checkpoint', '')
                checkpoints.append(job_name)
            
            return checkpoints
            
        except Exception as e:
            logger.error(f"Erreur lors du listage des checkpoints: {e}")
            return []
    
    def get_checkpoint_info(self, job_name: str) -> Optional[Dict[str, Any]]:
        """
        Obtient les informations sur un checkpoint sans le charger entièrement
        
        Args:
            job_name: Nom du job
        
        Returns:
            Métadonnées du checkpoint
        """
        state = self.load_checkpoint(job_name)
        if state and '_checkpoint_metadata' in state:
            return state['_checkpoint_metadata']
        return None


class ProgressTracker:
    """Classe pour suivre la progression d'une tâche avec checkpoints"""
    
    def __init__(self, job_name: str, total: int, checkpoint_interval: int = 1000):
        self.job_name = job_name
        self.total = total
        self.checkpoint_interval = checkpoint_interval
        self.checkpoint_manager = CheckpointManager()
        
        # Charger l'état précédent si existe
        state = self.checkpoint_manager.load_checkpoint(job_name)
        if state:
            self.current = state.get('current', 0)
            self.processed = state.get('processed', 0)
            self.failed = state.get('failed', 0)
            self.start_time = datetime.fromisoformat(state.get('start_time', datetime.now().isoformat()))
            logger.info(f"Reprise depuis checkpoint: {self.current}/{self.total}")
        else:
            self.current = 0
            self.processed = 0
            self.failed = 0
            self.start_time = datetime.now()
            logger.info(f"Nouveau tracking initialisé: 0/{self.total}")
    
    def update(self, increment: int = 1, success: bool = True) -> None:
        """
        Met à jour la progression
        
        Args:
            increment: Nombre d'éléments traités
            success: Si le traitement a réussi
        """
        self.current += increment
        
        if success:
            self.processed += increment
        else:
            self.failed += increment
        
        # Sauvegarder checkpoint si intervalle atteint
        if self.current % self.checkpoint_interval == 0:
            self._save_state()
            logger.info(f"Progression: {self.current}/{self.total} ({self.get_progress_percentage():.1f}%)")
    
    def _save_state(self) -> None:
        """Sauvegarde l'état actuel"""
        state = {
            'current': self.current,
            'total': self.total,
            'processed': self.processed,
            'failed': self.failed,
            'start_time': self.start_time.isoformat(),
            'last_update': datetime.now().isoformat()
        }
        self.checkpoint_manager.save_checkpoint(self.job_name, state)
    
    def complete(self) -> None:
        """Marque la tâche comme complète et nettoie le checkpoint"""
        duration = (datetime.now() - self.start_time).total_seconds()
        logger.info(f"Tâche {self.job_name} terminée: {self.processed} réussis, {self.failed} échoués en {duration:.2f}s")
        self.checkpoint_manager.clear_checkpoint(self.job_name)
    
    def get_progress_percentage(self) -> float:
        """Retourne le pourcentage de progression"""
        if self.total == 0:
            return 100.0
        return (self.current / self.total) * 100
    
    def get_summary(self) -> Dict[str, Any]:
        """Retourne un résumé de la progression"""
        duration = (datetime.now() - self.start_time).total_seconds()
        return {
            'current': self.current,
            'total': self.total,
            'processed': self.processed,
            'failed': self.failed,
            'progress_percentage': self.get_progress_percentage(),
            'duration_seconds': duration,
            'records_per_second': self.current / duration if duration > 0 else 0
        }


# Exemple d'utilisation
if __name__ == "__main__":
    # Test CheckpointManager
    manager = CheckpointManager()
    
    # Sauvegarder un checkpoint
    manager.save_checkpoint("test_job", {
        'current_line': 1000,
        'file': 'data.csv',
        'status': 'in_progress'
    })
    
    # Charger le checkpoint
    state = manager.load_checkpoint("test_job")
    print("État chargé:", state)
    
    # Test ProgressTracker
    tracker = ProgressTracker("ingestion_test", total=10000, checkpoint_interval=100)
    
    for i in range(100):
        tracker.update(success=True)
    
    print("Résumé:", tracker.get_summary())
    tracker.complete()

