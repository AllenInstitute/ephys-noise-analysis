@rem %USERPROFILE% = C:\Users\%USERNAME%
call %USERPROFILE%\Anaconda3\Scripts\activate.bat
call activate ephys-noise-analysis-env
call python src\generate_power_60hz_metrics.py
call conda deactivate