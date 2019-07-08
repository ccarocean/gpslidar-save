from dataclasses import dataclass
from collections import defaultdict


# Lookup table for GPS codes
_LOOKUP_GPS = {0: 'G', 1: 'S', 2: 'E', 3: 'C', 6: 'R'}


@dataclass(frozen=True)
class RxmRawxData:
    """ Dataclass for data from each satellite. """
    prMeas: float
    cpMeas: float
    doMeas: float
    gnssId: int
    svId: int
    sigId: int
    cno: int
    key: str


class RxmRawx:
    """ Receive packet for raw GPS data from multiple GNSS types. """
    id = 0x1502
    longname = 'Multi GNSS raw measurement data'

    def __init__(self, rcv_tow, week, leap_s, measurements):
        self._rcvTow = rcv_tow
        self._week = week
        self._leapS = leap_s

        dc = []

        for i in measurements:
            id_ = _LOOKUP_GPS[i[4]]
            if id_ == 'R' and i[5] == 255:
                key = ''
            else:
                if id_ == 'S':
                    id2 = i[5] - 100
                else:
                    id2 = i[5]
                key = f'{id_}{id2:02d}'

            dc.append(RxmRawxData(i[1], i[2], i[3], i[4], i[5], i[6], i[7], key))

        dd = defaultdict(list)
        self._satellites = []
        for i in dc:
            dd[i.key].append(i)
        for i in dd.items():
            i[1].sort(key=lambda x: x.sigId)
            self._satellites.append(i[1])
        self._satellites.sort(key=lambda x: x[0].key)

    def __str__(self):
        return (f'Received Packet:     {self.longname}, ID: {self.id}\n' 
                f'Receiver Time of Week:    {self.rcvTow}\n' 
                f'Week Number               {self.week}\n' 
                f'Leap Second offset:       {self.leapS}\n'
                f'Satellite Measurements:   {self.satellites}\n')

    @property
    def rcvTow(self):
        return self._rcvTow
    
    @property
    def week(self):
        return self._week
    
    @property
    def leapS(self):
        return self._leapS

    @property
    def satellites(self):
        return self._satellites
