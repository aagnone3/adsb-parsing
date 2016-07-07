"""
ADSB SBS-1 parsing utilties
Derived from the protocol defined at the link below
    http://woodair.net/sbs/Article/Barebones42_Socket_Data.htm
"""


class SBS1Entry(object):
    # Static constant members
    EXPECTED_NUM_MSGS = 4
    VALID_MESSAGE_SETS = [[1, 2, 6],
                          [1, 3, 4, 6]]

    def __init__(self, data):
        self.data = ["", "", -1, -1, "", -1, "", "", "", "", "", -1, 0.0, 0.0, 0.0, 0.0, -1, -1,
                     False, False, False, False]
        self.unique_id = self.data[4] = data[4]
        self.message_type = self.data[0] = data[0]
        self.data[2] = SBS1Entry.sbs1_int(self.data[2])
        self.data[3] = SBS1Entry.sbs1_int(self.data[3])
        self.data[5] = SBS1Entry.sbs1_int(self.data[5])
        self.n_messages = 0
        self.seen_message_types = []

    @staticmethod
    def sbs1_int(value):
        return int(value) if type(value) is int or value.isnumeric() else -1

    @staticmethod
    def sbs1_boolean(value):
        return 1 if value == "-1" else 0


class TransmissionMsgEntry(SBS1Entry):
    def __init__(self, data):
        SBS1Entry.__init__(self, data)
        self.transmission_type = self.data[1]
        # Set the transmission type field to 0, since we will be merging
        # multiple messages together to form the complete entries.
        self.data[1] = "0"
        self.more_data(data)

    def more_data(self, data):
        transmission_type = int(data[1])
        if self.unique_id == data[4]:
            self.n_messages += 1
            self.seen_message_types.append(transmission_type)
        if transmission_type == 1:
            # MSG 1: ES Identification and Category Message
            self.data[10] = data[10]
        elif transmission_type == 3:
            # MSG 3: ES Airborne Position Message
            self.data[11] = int(data[11])
            self.data[14] = float(data[14])
            self.data[15] = float(data[15])
            self.data[18] = self.sbs1_boolean(data[18])
            self.data[19] = self.sbs1_boolean(data[19])
            self.data[20] = self.sbs1_boolean(data[20])
            self.data[21] = self.sbs1_boolean(data[21])
        elif transmission_type == 4:
            # MSG 4: ES Airborne Velocity Message
            self.data[12] = float(data[12])
            self.data[13] = float(data[13])
            self.data[16] = SBS1Entry.sbs1_int(data[16])
        elif transmission_type == 6:
            # MSG 6: Surveillance ID Message
            self.data[11] = int(data[11])
            self.data[17] = data[17]
            self.data[18] = self.sbs1_boolean(data[18])
            self.data[19] = self.sbs1_boolean(data[19])
            self.data[20] = self.sbs1_boolean(data[20])
            self.data[21] = self.sbs1_boolean(data[21])
