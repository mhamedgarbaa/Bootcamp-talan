"""
Script pour télécharger le dataset Olist depuis Kaggle
"""
import os
import kaggle
from pathlib import Path

def download_olist_dataset():
    """Télécharge le dataset Olist depuis Kaggle"""
    
    # Créer le dossier data/raw
    data_path = Path("data/raw")
    data_path.mkdir(parents=True, exist_ok=True)
    
    print("📥 Downloading Olist Brazilian E-Commerce dataset from Kaggle...")
    print("⚠️  Make sure you have kaggle.json configured in ~/.kaggle/")
    
    try:
        # Téléchargement via Kaggle API
        kaggle.api.dataset_download_files(
            'olistbr/brazilian-ecommerce',
            path=data_path,
            unzip=True
        )
        
        print(f"✅ Dataset downloaded to {data_path}")
        
        # Lister les fichiers téléchargés
        files = list(data_path.glob("*.csv"))
        print(f"\n📁 Downloaded {len(files)} files:")
        for file in files:
            size_mb = file.stat().st_size / (1024 * 1024)
            print(f"  - {file.name} ({size_mb:.2f} MB)")
    
    except Exception as e:
        print(f"❌ Error downloading dataset: {e}")
        print("\n💡 Configuration Kaggle:")
        print("1. Créer un compte sur https://www.kaggle.com")
        print("2. Aller dans Account -> API -> Create New API Token")
        print("3. Placer kaggle.json dans:")
        print("   - Windows: C:\\Users\\<username>\\.kaggle\\kaggle.json")
        print("   - Linux/Mac: ~/.kaggle/kaggle.json")

if __name__ == "__main__":
    download_olist_dataset()
