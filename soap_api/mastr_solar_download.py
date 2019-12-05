#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BNetzA - MaStR Download - Solar

Read data from MaStR API and write to CSV files.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli; Bachibouzouk; solar-c"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.9.0"

from soap_api.sessions import mastr_session
from soap_api.utils import split_to_sublists, get_data_version, write_to_csv, remove_csv, read_power_units

import multiprocessing as mp
from multiprocessing.pool import ThreadPool 
import pandas as pd
import numpy as np
import datetime
import os
import numpy as np
from zeep.helpers import serialize_object
from functools import partial

import time
import logging
log = logging.getLogger(__name__)

""" import variables """
from soap_api.utils import fname_power_unit, fname_solar, fname_solar_unit, fname_solar_eeg, fname_solar_eeg_unit

"""SOAP API"""
client, client_bind, token, user = mastr_session()
api_key = token
my_mastr = user


def get_power_unit_solar(mastr_unit_solar):
    """Get Solareinheit from API using GetEinheitSolar.

    Parameters
    ----------
    mastr_unit_solar : object
        Solar from EinheitMastrNummerId.

    Returns
    -------
    unit_solar : DataFrame
        Solareinheit.
    """
    #with mp.Lock():
    #   log.info('downloading data unit... %s', mastr_unit_solar)

    data_version = get_data_version()
    
    try:
        c = client_bind.GetEinheitSolar(apiKey=api_key,
                                    marktakteurMastrNummer=my_mastr,
                                    einheitMastrNummer=mastr_unit_solar)
        s = serialize_object(c)
        df = pd.DataFrame(list(s.items()), )
        unit_solar = df.set_index(list(df.columns.values)[0]).transpose()
        unit_solar.reset_index()
        unit_solar.index.names = ['lid']
        unit_solar['version'] = data_version
        unit_solar['timestamp'] = str(datetime.datetime.now())
    except Exception as e:
        return unit_solar
    
    return unit_solar


def read_unit_solar(csv_name):
    """Read Solareinheit from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    unit_solar : DataFrame
        Solareinheit.
    """
    # log.info(f'Read data from {csv_name}')
    unit_solar = pd.read_csv(csv_name, header=0, encoding='utf-8', sep=';', index_col=False,
                             dtype={'lid': int,
                                    'Ergebniscode': str,
                                    'AufrufVeraltet': str,
                                    'AufrufLebenszeitEnde': str,
                                    'AufrufVersion': str,
                                    'EinheitMastrNummer': str,
                                    'DatumLetzteAktualisierung': str,
                                    'LokationMastrNummer': str,
                                    'NetzbetreiberpruefungStatus': str,
                                    'NetzbetreiberpruefungDatum': str,
                                    'AnlagenbetreiberMastrNummer': str,
                                    'Land': str,
                                    'Bundesland': str,
                                    'Landkreis': str,
                                    'Gemeinde': str,
                                    'Gemeindeschluessel': str,
                                    'Postleitzahl': str,
                                    'Gemarkung': str,
                                    'FlurFlurstuecknummern': str,
                                    'Strasse': str,
                                    'StrasseNichtGefunden': str,
                                    'Hausnummer': str,
                                    'HausnummerNichtGefunden': str,
                                    'Adresszusatz': str,
                                    'Ort': str,
                                    'Laengengrad': str,
                                    'Breitengrad': str,
                                    'UtmZonenwert': str,
                                    'UtmEast': str,
                                    'UtmNorth': str,
                                    'GaussKruegerHoch': str,
                                    'GaussKruegerRechts': str,
                                    'Meldedatum': str,
                                    'GeplantesInbetriebnahmedatum': str,
                                    'Inbetriebnahmedatum': str,
                                    'DatumEndgueltigeStilllegung': str,
                                    'DatumBeginnVoruebergehendeStilllegung': str,
                                    'DatumWiederaufnahmeBetrieb': str,
                                    'EinheitBetriebsstatus': str,
                                    'BestandsanlageMastrNummer': str,
                                    'NichtVorhandenInMigriertenEinheiten': str,
                                    'NameStromerzeugungseinheit': str,
                                    'Weic': str,
                                    'WeicDisplayName': str,
                                    'Kraftwerksnummer': str,
                                    'Energietraeger': str,
                                    'Bruttoleistung': float,
                                    'Nettonennleistung': float,
                                    'AnschlussAnHoechstOderHochSpannung': str,
                                    'Schwarzstartfaehigkeit': str,
                                    'Inselbetriebsfaehigkeit': str,
                                    'Einsatzverantwortlicher': str,
                                    'FernsteuerbarkeitNb': str,
                                    'FernsteuerbarkeitDv': str,
                                    'FernsteuerbarkeitDr': str,
                                    'Einspeisungsart': str,
                                    'PraequalifiziertFuerRegelenergie': str,
                                    'GenMastrNummer': str,
                                    'zugeordneteWirkleistungWechselrichter': str,
                                    'GemeinsamerWechselrichterMitSpeicher': str,
                                    'AnzahlModule': str,
                                    'Lage': str,
                                    'Leistungsbegrenzung': str,
                                    'EinheitlicheAusrichtungUndNeigungswinkel': str,
                                    'Hauptausrichtung': str,
                                    'HauptausrichtungNeigungswinkel': str,
                                    'Nebenausrichtung': str,
                                    'NebenausrichtungNeigungswinkel': str,
                                    'InAnspruchGenommeneFlaeche': str,
                                    'ArtDerFlaeche': str,
                                    'InAnspruchGenommeneAckerflaeche': str,
                                    'Nutzungsbereich': str,
                                    'EegMastrNummer': str,
                                    'version': str,
                                    'timestamp': str})
    # log.info(f'Finished reading data from {csv_name}')
    return unit_solar


def get_unit_solar_eeg(mastr_solar_eeg):
    """Get EEG-Anlage-Solar from API using GetAnlageEegSolar.

    Parameters
    ----------
    mastr_solar_eeg : str
        MaStR EEG Nr.

    Returns
    -------
    unit_solar_eeg : DataFrame
        EEG-Anlage-Solar.
    """
    data_version = get_data_version()
    try:
        c = client_bind.GetAnlageEegSolar(apiKey=api_key,
                                      marktakteurMastrNummer=my_mastr,
                                      eegMastrNummer=mastr_solar_eeg)
        s = serialize_object(c)
        df = pd.DataFrame(list(s.items()), )
        unit_solar_eeg = df.set_index(list(df.columns.values)[0]).transpose()
        unit_solar_eeg.reset_index()
        unit_solar_eeg.index.names = ['lid']
        unit_solar_eeg["version"] = data_version
        unit_solar_eeg["timestamp"] = str(datetime.datetime.now())
    except Exception as e:
        return unit_solar_eeg
    return unit_solar_eeg


def read_unit_solar_eeg(csv_name):
    """
    Encode and read EEG-Anlage-Solar from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    unit_solar_eeg : DataFrame
        EEG-Anlage-Solar
    """
    # log.info(f'Read data from {csv_name}')
    unit_solar_eeg = pd.read_csv(csv_name, header=0, sep=';', index_col=False, encoding='utf-8',
                                 dtype={'lid': int,
                                        'Ergebniscode': str,
                                        'AufrufVeraltet': str,
                                        'AufrufLebenszeitEnde': str,
                                        'AufrufVersion': str,
                                        'Meldedatum': str,
                                        'DatumLetzteAktualisierung': str,
                                        'EegInbetriebnahmedatum': str,
                                        'EegMastrNummer': str,
                                        'InanspruchnahmeZahlungNachEeg': str,
                                        'AnlagenschluesselEeg': str,
                                        'AnlagenkennzifferAnlagenregister': str,
                                        'InstallierteLeistung': str,
                                        'RegistrierungsnummerPvMeldeportal': str,
                                        'MieterstromZugeordnet': str,
                                        'MieterstromMeldedatum': str,
                                        'MieterstromErsteZuordnungZuschlag': str,
                                        'AusschreibungZuschlag': str,
                                        'ZugeordneteGebotsmenge': str,
                                        'Zuschlagsnummer': str,
                                        'AnlageBetriebsstatus': str,
                                        'VerknuepfteEinheit': str,
                                        'version': str,
                                        'timestamp': str})
    # log.info(f'Finished reading data from {csv_name}')
    return unit_solar_eeg


def setup_power_unit_solar(overwrite=True, eeg=False):
    """Setup file for Stromerzeugungseinheit-Solar.

    Check if file with Stromerzeugungseinheit-Solar exists. Create if not exists.
    Load Stromerzeugungseinheit-Solar from file if exists.

    Returns
    -------
    power_unit_solar : DataFrame
        Stromerzeugungseinheit-Solar.
    """
    data_version = get_data_version()
    if overwrite and not eeg:
            remove_csv(fname_solar)
            remove_csv(fname_solar_unit)
    elif overwrite and eeg:
            remove_csv(fname_solar_eeg)
            remove_csv(fname_solar_eeg_unit)
    if os.path.isfile(fname_power_unit):
        power_unit = read_power_units(fname_power_unit)
        if not power_unit.empty:
            power_unit = power_unit.drop_duplicates()
            power_unit_solar = power_unit[power_unit.Einheittyp == 'Solareinheit']
            power_unit_solar.index.names = ['see_id']
            power_unit_solar.reset_index()
            power_unit_solar.index.names = ['id']
            if not eeg:
                write_to_csv(fname_solar_unit, power_unit_solar)       
            else:
                write_to_csv(fname_solar_eeg_unit, power_unit_solar)           
            power_unit.iloc[0:0]
            return power_unit_solar
        else:
            log.info('no solarunits found')
            return pd.DataFrame()
    return power_unit_solar


def download_unit_solar(overwrite=True):
    """Download Solareinheit.

    Existing units: 31543 (2019-02-10)
    """
    start_from = 0
    log.info('download unit solar..')
    unit_solar = setup_power_unit_solar(overwrite, eeg=False)
    unit_solar_list = unit_solar['EinheitMastrNummer'].values.tolist()
    unit_solar_list_len = len(unit_solar_list)
    log.info(f'Download MaStR Solar')
    log.info(f'Number of unit_solar: {unit_solar_list_len}')

    for i in range(start_from, unit_solar_list_len, 1):
        try:
            unit_solar = get_power_unit_solar(unit_solar_list[i])
            write_to_csv(fname_solar, unit_solar)
        except:
            log.exception(f'Download failed unit_solar ({i}): {unit_solar_list[i]}')


def download_parallel_unit_solar(
        start_from=0,
        n_entries=1,
        parallelism=300,
        cpu_factor=1,
        overwrite=True
):
    """Download GetEinheitSolar with parallel process


    Arguments
    ---------
    start_from : int
        Start index in the power_unit_list.
    n_entries : int
        Number of entries to download
    parallelism : int
        number of threads
    cpu_factor : float
        multiplies the number of processes depending on available cpu units
    overwrite : bool
        decide wether the existing files should be overwritten or not


    Existing units: 31543 (2019-02-10)

    """
    global proc_list
    split_solar_list = []

    unit_solar = setup_power_unit_solar(overwrite, eeg=False) 
    if unit_solar.empty:
        return
    unit_solar_list = unit_solar['EinheitMastrNummer'].values.tolist()
    unit_solar_list_len = len(unit_solar_list)
    # check wether user input
    if n_entries is 1:
        n_entries = unit_solar_list_len
    # check wether to download more entries than solareinheiten in unit_solar_list starting at start_from
    if n_entries > (unit_solar_list_len-start_from):
        n_entries = unit_solar_list_len-start_from
    log.info('Found %s solar units', n_entries)
    end_at = start_from+n_entries
    cpu_count = mp.cpu_count()*cpu_factor
    process_pool = mp.Pool(processes=cpu_count)
    t = time.time()
    proc_list = split_to_sublists(unit_solar_list[start_from:end_at], len(unit_solar_list[start_from:end_at]),cpu_count)
    print("This may take a moment. Processing {} data batches.".format(len(proc_list)))
    try:
        partial(split_to_threads, parallelism=parallelism)
        unit_solar = process_pool.map(split_to_threads, proc_list)
        process_pool.close()
        process_pool.join()
        if not eeg:
            write_to_csv(fname_solar, unit_solar)
        else:
            write_to_csv(fname_solar_eeg, unit_solar)
    except Exception as e:
        log.error(e)
    log.info('time needed %s', time.time()-t)




def download_parallel_unit_solar_eeg(
        start_from=0,
        n_entries=1,
        parallelism=300,
        cpu_factor=1,
        overwrite=True
):

    """Download GetAnlageEegSolar with parallel process


    Arguments
    ---------
    start_from : int
        Start index in the power_unit_list.
    n_entries : int
        Number of entries to download
    parallelism : int
        number of threads
    cpu_factor : float
        multiplies the number of processes depending on available cpu units
    overwrite : bool
        decide wether the existing files should be overwritten or not


    Existing units: 31543 (2019-02-10)

    Existing units: 31543 (2019-02-10)
    """

    global proc_list
    split_solar_list = []

    unit_solar = setup_power_unit_solar(overwrite, eeg=True) 
    if unit_solar.empty:
        return
    unit_solar_list = unit_solar['EegMastrNummer'].values.tolist()
    unit_solar_list_len = len(unit_solar_list)
    # check wether user input
    if n_entries is 1:
        n_entries = unit_solar_list_len
    # check wether to download more entries than solareinheiten in unit_solar_list starting at start_from
    if n_entries > (unit_solar_list_len-start_from):
        n_entries = unit_solar_list_len-start_from
    log.info('Found %s solar units eeg', n_entries)
    end_at = start_from+n_entries
    cpu_count = mp.cpu_count()*cpu_factor
    process_pool = mp.Pool(processes=cpu_count)
    t = time.time()
    proc_list = split_to_sublists(unit_solar_list[start_from:end_at],len(unit_solar_list[start_from:end_at]),cpu_count)
    print("This may take a moment. Processing {} data eeg batches.".format(len(proc_list)))
    try:
        partial(split_to_threads_eeg, parallelism=parallelism)
        unit_solar = process_pool.map(split_to_threads_eeg, proc_list)
        process_pool.close()
        process_pool.join()
        write_to_csv(fname_solar_eeg, unit_solar)
    except Exception as e:
        log.error(e)
    log.info('time needed %s', time.time()-t)


def split_to_threads(sublist,parallelism=100):
    """ Maps sublist variables to function get_power_unit_solar on parallel threads (number = parallelism)

    Parameters
    ----------
    sublist : list
        list to process in parallel
    parallelism : int
        number of threads
    """
    pool = ThreadPool(processes=parallelism)
    results = pool.map(get_power_unit_solar, sublist)
    pool.close()
    pool.join()
    return results


def split_to_threads_eeg(sublist,parallelism=100):
    """ Maps sublist variables to function get_unit_solar_eeg on parallel threads (number = parallelism)

    Parameters
    ----------
    sublist : list
        list to process in parallel
    parallelism : int
        number of threads
    """
    pool = ThreadPool(processes=parallelism)
    results = pool.map(get_unit_solar_eeg, sublist)
    pool.close()
    pool.join()
    return results


def download_unit_solar_eeg(overwrite=True):
    """Download unit_solar_eeg using GetAnlageEegSolar request.

    Parameters
    ----------
    sublist : list
        list to process in parallel
    parallelism : int
        number of threads
    """
    data_version = get_data_version()
    unit_solar = setup_power_unit_solar(overwrite, eeg=True)

    unit_solar_list = unit_solar['EegMastrNummer'].values.tolist()
    unit_solar_list_len = len(unit_solar_list)

    for i in range(0, unit_solar_list_len, 1):
        try:
            unit_solar_eeg = get_unit_solar_eeg(unit_solar_list[i])
            write_to_csv(fname_solar_eeg, unit_solar_eeg)
        except:
            log.exception(f'Download failed unit_solar_eeg ({i}): {unit_solar_list[i]}')