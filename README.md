# py_scripts

*Агрегация данных по показателям из столбца CL_INDICATORS DataFrame df, содержащего JSON-объекты*
```
summed_indicators = defaultdict(float)


indicator_codes_map = {
    'ChislFiz': 'Численность работающих застрахованных лиц по обязательному социальному страхованию от несчастных случаев на производстве и профессиональных заболеваний',
    'CHislSr': 'Среднесписочная численность работников',
    'CHislInv': 'Численность работающих инвалидов',
    'CHislVred': 'Численность работников, занятых на работах с вредными и (или) опасными производственными факторами',
    'KolStrakhPredostRaschVsegoNspz': 'Число страхователей, представивших раздел 2 "Сведения о начисленных страховых взносах на обязательное социальное страхование от несчастных случаев на производстве и профессиональных заболеваний" формы ЕФС-1',
    'KolStrakhNaUchete': 'Число страхователей, состоящих на учете на конец отчетного периода',
    'KolStrakhDobrovol': 'Число страхователей, добровольно вступивших в правоотношения по обязательному социальному страхованию на случай временной нетрудоспособности и в связи с материнством'
}

# Перебор каждой строки в DataFrame
for _, row in df.iterrows():
    indicators = json.loads(row['CL_INDICATORS'])
    
    # Словарь для отслеживания, какие коды индикаторов уже обработаны
    processed_indicators = set()

    for indicator in indicators:
        indicator_code = indicator.get('indicatorCode')
        source_id = indicator.get('indSourceId')
        value = indicator.get('value', 0) or 0  # Установить значение 0, если оно отсутствует

        # Проверка, есть ли код индикатора среди требуемых
        if indicator_code in indicator_codes_map:
            # Проверка, был ли этот индикатор уже обработан
            if indicator_code not in processed_indicators:
                # Если индикатор из источника 2, суммировать его напрямую
                if source_id == 2:
                    summed_indicators[indicator_code] += value
                    processed_indicators.add(indicator_code)
                # Если источник 2 не найден, проверить и суммировать из источника 1
                elif source_id == 1 and indicator_code not in processed_indicators:
                    summed_indicators[indicator_code] += value

# Создание DataFrame с суммированными результатами и их описаниями
results_df = pd.DataFrame([
    {'Indicator Code': code, 'Description': indicator_codes_map[code], 'Total Value': summed_indicators[code]}
    for code in indicator_codes_map
])

# Сохранение результатов на новый лист в исходном Excel файле
with pd.ExcelWriter(file_path, engine='openpyxl', mode='a') as writer:
    results_df.to_excel(writer, sheet_name='Summed Indicators', index=False)

file_path
```
*Объединение квартальных данных по itemId с суммированием показателей*
```
import os
import pandas as pd
import json

# Папка с квартальными файлами
folder_path = "data/quarterly_reports/"
output_file = "data/merged_quarterly.xlsx"

files = [f for f in os.listdir(folder_path) if f.endswith('.xlsx') or f.endswith('.csv')]
indicators = ["ChislRab", "StrahVznos", "KolDogov"]

all_data = []

for file in files:
    file_path = os.path.join(folder_path, file)
    try:
        # Определяем, CSV или Excel
        if file.endswith('.xlsx'):
            df = pd.read_excel(file_path)
        else:
            df = pd.read_csv(file_path)

        # Проверяем, есть ли нужные колонки
        required_columns = {"itemId"} | set(indicators)
        if not required_columns.issubset(df.columns):
            print(f"⚠️ Пропущен файл {file}: нет нужных колонок {required_columns - set(df.columns)}")
            continue

        # "2024_Q1.xlsx" → "2024 Q1"
        quarter = file.split('.')[0].replace('_', ' ')
        df["Quarter"] = quarter  # Добавляем колонку с кварталом

        all_data.append(df)

    except Exception as e:
        print(f"❌ Ошибка при обработке {file}: {e}")

merged_df = pd.concat(all_data, ignore_index=True)
aggregated_df = merged_df.groupby("itemId", as_index=False)[indicators].sum()

aggregated_df["Indicators"] = aggregated_df.apply(lambda row: json.dumps({
    "ChislRab": row["ChislRab"],
    "StrahVznos": row["StrahVznos"],
    "KolDogov": row["KolDogov"]
}, ensure_ascii=False), axis=1)

final_df = aggregated_df[["itemId", "Indicators"]]
final_df.to_excel(output_file, index=False)
```
