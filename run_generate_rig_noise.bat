@rem %USERPROFILE% = C:\Users\%USERNAME%
call %USERPROFILE%\Anaconda3\Scripts\activate.bat
call activate ephys-noise-analysis-env
call python src\generate_rig_noise.py
call conda deactivate