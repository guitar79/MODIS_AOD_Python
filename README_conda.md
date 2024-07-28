
# MODIS_AOD_Python

# Windows
## 가상환경 리스트 출력
conda env list

## anaconda 가상환경 만들기
conda create -n MODIS_AOD_Python_env

## activate 가상환경 시작
conda activate MODIS_AOD_Python_env

## deactivate 가상환경 종료
conda deactivate

## install module
conda install -c conda-forge pyhdf basemap basemap-data basemap-data-hires 

## 가상환경 내보내기 (export)
conda env export > MODIS_AOD_Python_env.yaml

## .yaml 파일로 새로운 가상환경 만들기
conda env create -f MODIS_AOD_Python_env.yaml

## 가상환경 제거하기
conda env remove -n MODIS_AOD_Python_env