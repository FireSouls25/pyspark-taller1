from ucimlrepo import fetch_ucirepo
import pandas as pd
import os

def descargar_online_retail():
    print("Descargando dataset Online Retail desde UCI...")
    
    online_retail = fetch_ucirepo(id=352)
    
    X = online_retail.data.features
    y = online_retail.data.targets
    
    if y is not None:
        df = pd.concat([X, y], axis=1)
    else:
        df = X
    
    os.makedirs("online+retail", exist_ok=True)
    
    output_path = "online+retail/Online Retail.xlsx"
    df.to_excel(output_path, index=False)
    
    print(f"Dataset guardado en: {output_path}")
    print(f"Total de registros: {len(df)}")
    print(f"Columnas: {list(df.columns)}")
    
    return df

if __name__ == "__main__":
    descargar_online_retail()
