import pandas, pytz, discord, dotenv, os, sys, traceback, re, requests, asyncio
from datetime import datetime, timezone, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

import gspread
from google.oauth2 import service_account

# taken straight from the python docs

#import linecache
#import os
#import tracemalloc

#def display_top(snapshot, key_type='lineno', limit=10):
#    snapshot = snapshot.filter_traces((
#        tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
#        tracemalloc.Filter(False, "<unknown>"),
#    ))
#    top_stats = snapshot.statistics(key_type)
#
#    print("Top %s lines" % limit)
#    for index, stat in enumerate(top_stats[:limit], 1):
#        frame = stat.traceback[0]
#        print("#%s: %s:%s: %.1f KiB"
#              % (index, frame.filename, frame.lineno, stat.size / 1024))
#        line = linecache.getline(frame.filename, frame.lineno).strip()
#        if line:
#            print('    %s' % line)
#
#    other = top_stats[limit:]
#    if other:
#        size = sum(stat.size for stat in other)
#        print("%s other: %.1f KiB" % (len(other), size / 1024))
#    total = sum(stat.size for stat in top_stats)
#    print("Total allocated size: %.1f KiB" % (total / 1024))
#
#tracemalloc.start()


client = None # client api setup
StartingRowIdentifier = "Date ET"
ETColumnIdentifier = "Time (ET)"
UTCColumnIdentifier = "Time (UTC)"
CommentatorIdentifier = "Commentators"
TrackerIdentifier = "Trackers"
EasternTimeZone = pytz.timezone("US/Eastern")
SuccessChatChannelID = 1446553558379139113

load_dotenv()

CurrentBot = "Main" # Main or Beta, determines the bot input params # change this line if you're swapping between beta and main

if CurrentBot == "Main":
    token = os.getenv("BotToken")
    WWRVolunteersChatChannelID = int(os.getenv("WWRVolunteerChatChannelID")) # channel id for #volunteer-chat
    CommentatorRoleID = os.getenv("CommentatorRoleID") # role id for comms
    TrackerRoleID = os.getenv("TrackerRoleID") # role id for trackers
    TargetSheet = os.getenv("TargetSheetWebsite") # url to target
    APITargetSheet = os.getenv("TargetSheetID") # sheet to target with forced recalculation, part of the TargetSheetWebsite
    ServiceAcc = os.getenv("MainServiceJSONFilePath") # file path where the service acc credential json is located
elif CurrentBot == "Beta":
    token = os.getenv("BetaBotToken")
    TargetSheet = "https://docs.google.com/spreadsheets/d/1H19xsapwJxxqxcJU2EbH82zsBqltpYVaQ1I7Kj8Pbp0/export?format=csv&id=1H19xsapwJxxqxcJU2EbH82zsBqltpYVaQ1I7Kj8Pbp0&gid=0"
    CommentatorRoleID = "<@&1411103550595137536>"
    TrackerRoleID = "<@&1411103607075635220>"
    WWRVolunteersChatChannelID = 1409607946786046055
    APITargetSheet = "1H19xsapwJxxqxcJU2EbH82zsBqltpYVaQ1I7Kj8Pbp0"
    ServiceAcc = os.getenv("BetaServiceJSONFilePath")
else:
    pass

ErrorLogChannelID = int(os.getenv("ErrorLogChannelID")) # error logs get put here

AdvancePingTimeframe = 48 # hours before race that it should ping # change this line if you're swapping between beta and main

StoredMessagesFile = os.path.join(os.path.dirname(os.path.abspath(__file__)), "StoredMessageIDs.txt")

FullRaceList = [] # holds the ScheduledRace objects

class ScheduledRace:
    def __init__(self, MessageInfo):
        self.UUID = MessageInfo[0]
        self.name = MessageInfo[1]
        self.time = MessageInfo[2]
        self.neededcomms = MessageInfo[3]
        if isinstance(self.neededcomms, str):
            self.neededcomms = int(self.neededcomms)
        self.neededtrackers = MessageInfo[4]
        if isinstance(self.neededtrackers, str):
            self.neededtrackers = int(self.neededtrackers)
        try:
            self.messageID = MessageInfo[5]
        except:
            self.messageID = None

    async def StoreMessage(self):
        with open(StoredMessagesFile, "a", encoding = "utf-8") as file:
            file.write(f"[{self.UUID}, {self.name}, {self.time}, {self.neededcomms}, {self.neededtrackers}, {self.messageID}]\n")

with open(StoredMessagesFile, "r", encoding = "utf-8") as file:
    MessageInfo = file.read().splitlines() # holds all the message ids for previously sent messages in case we need to change them later
    file.close()

for race in MessageInfo:
    race = race[1:-1]
    FullRaceList.append(ScheduledRace(race.split(", ")))

PingedMatches = [race.UUID for race in FullRaceList]

bot = discord.Client(intents=discord.Intents.default())

Scheduler = AsyncIOScheduler()

async def CheckSheet():

    try:
        global KeyColumnIndexes

        await RefreshSheet() # put a breakpoint here if you want to edit the sheet then have it read it for debugging

        await TransmitMessage(f"Sheet successfully refreshed at {datetime.now()}", SuccessChatChannelID)
        print(f"Sheet successfully refreshed at {datetime.now()}")
        RawRaceList = pandas.read_csv(TargetSheet)

        NewHeaderRowIndex = None

        for row in RawRaceList.itertuples():
            if NewHeaderRowIndex is None:
                for col in range(len(row)):
                    if isinstance(row[col], str) and row[col].strip() == StartingRowIdentifier:
                        NewHeaderRowIndex = row.Index
                        break
            else:
                break

        CleanedRaceList = RawRaceList.iloc[NewHeaderRowIndex:]
        CleanedRaceList = CleanedRaceList[1:]
        CleanedRaceList.columns = RawRaceList.iloc[NewHeaderRowIndex].str.strip()
        CleanedRaceList = CleanedRaceList.replace(r"\n", " ", regex = True)

        DateColumnIndex = CleanedRaceList.columns.get_loc(StartingRowIdentifier) + 1
        ETColumnIndex = CleanedRaceList.columns.get_loc(ETColumnIdentifier) + 1
        CommentatorIndex = CleanedRaceList.columns.get_loc(CleanedRaceList.columns[CleanedRaceList.columns.str.contains(CommentatorIdentifier, regex=False)][0]) + 1
        TrackerIndex = CleanedRaceList.columns.get_loc(CleanedRaceList.columns[CleanedRaceList.columns.str.contains(TrackerIdentifier, regex=False)][0]) + 1
        KeyColumnIndexes = [DateColumnIndex, ETColumnIndex, CommentatorIndex, TrackerIndex]
        
        CurrentTime = datetime.now(EasternTimeZone)
        MaxPingTime = CurrentTime + timedelta(hours = AdvancePingTimeframe)

        for row in CleanedRaceList.itertuples():
            try:
                MatchMonth = datetime.strptime(row[DateColumnIndex], "%b %d").month
                CurMonth = datetime.now().month
                if MatchMonth < CurMonth: # if the number of the month the match takes place in is less than the number of the current month, it means the match takes place next year, so we need to adjust it accordingly
                    ETFullDateTime = EasternTimeZone.localize(datetime.strptime(f"{str(row[DateColumnIndex])} {str(row[ETColumnIndex])} {datetime.now().year + 1}", "%b %d %I:%M%p %Y"))
                else: # otherwise, the match is in the current year
                    ETFullDateTime = EasternTimeZone.localize(datetime.strptime(f"{str(row[DateColumnIndex])} {str(row[ETColumnIndex])} {datetime.now().year}", "%b %d %I:%M%p %Y"))
            except:
                continue

            if ETFullDateTime <= MaxPingTime:
                DiscordMessage, UUID, Race = await CreateDiscordMessage(row, ETFullDateTime)
                if DiscordMessage != "" and Race != None:
                    MessageID = await TransmitMessage(DiscordMessage, WWRVolunteersChatChannelID)
                    await TransmitMessage(f"Volunteer notification successfully sent at {datetime.now()}", SuccessChatChannelID)
                    print(f"Volunteer notification successfully sent at {datetime.now()}")
                    Race.messageID = MessageID.id
                    await Race.StoreMessage()
                    FullRaceList.append(Race)

        #snapshot = tracemalloc.take_snapshot()
        #display_top(snapshot)

    except Exception as error:
        ErrorMessage = ""
        exc_type, exc_obj, tb = sys.exc_info()
        filename = tb.tb_frame.f_code.co_filename
        linenum = tb.tb_lineno
        line_text = traceback.extract_tb(tb)[-1].line
        ErrorMessage += f"Error Type: {exc_type.__name__}\n"
        ErrorMessage += f"Error Message: {error}\n"
        ErrorMessage += f"File: {filename}\n"
        ErrorMessage += f"Line #: {linenum}\n"
        ErrorMessage += f"Code Line: {line_text}\n"
        print(ErrorMessage)
        await TransmitError(ErrorMessage)
                
async def CreateDiscordMessage(row, ETtimestamp):
    global KeyColumnIndexes, TimeStamp
    UUID = str(ETtimestamp) + str(row.Race) + str(row.Round)
    FullMessagetoSend, RaceObject = "", None
    if UUID not in PingedMatches:
        MatchName = f"{str(row.Race)} {str(row.Round)}"
        TimeStamp = int(datetime.timestamp(ETtimestamp))
        RequiredCommentators, RequiredTrackers = await DetermineVolunteerReqs(row)
        if RequiredCommentators <= 0 and RequiredTrackers <= 0:
            return FullMessagetoSend, UUID, RaceObject
        elif RequiredCommentators > 0 and RequiredTrackers > 0:
            FullMessagetoSend += f"{CommentatorRoleID} {TrackerRoleID} {MatchName} is scheduled for <t:{TimeStamp}:f>, and we need {RequiredCommentators} commentator(s) and {RequiredTrackers} tracker(s).\nPlease sign up using this spreadsheet: <https://zsr.link/twwrvolunteer>"
        elif RequiredCommentators > 0 and RequiredTrackers <= 0:
            FullMessagetoSend += f"{CommentatorRoleID} {MatchName} is scheduled for <t:{TimeStamp}:f>, and we need {RequiredCommentators} commentator(s).\nPlease sign up using this spreadsheet: <https://zsr.link/twwrvolunteer>"
        elif RequiredCommentators <= 0 and RequiredTrackers > 0:
            FullMessagetoSend += f"{TrackerRoleID} {MatchName} is scheduled for <t:{TimeStamp}:f>, and we need {RequiredTrackers} tracker(s).\nPlease sign up using this spreadsheet: <https://zsr.link/twwrvolunteer>"
        RaceObject = ScheduledRace([UUID, MatchName, TimeStamp, RequiredCommentators, RequiredTrackers])
        PingedMatches.append(UUID)
    else:
        for race in FullRaceList:
            if race.UUID == UUID:
                RequiredCommentators, RequiredTrackers = await DetermineVolunteerReqs(row)
                if race.neededcomms != RequiredCommentators and max(race.neededcomms, RequiredCommentators) > 0:
                    await EditMessage(race, "commentator", RequiredCommentators)
                    race.neededcomms = RequiredCommentators
                    await race.StoreMessage()
                if race.neededtrackers != RequiredTrackers and max(race.neededtrackers, RequiredTrackers) > 0:
                    await EditMessage(race, "tracker", RequiredTrackers)
                    race.neededtrackers = RequiredTrackers
                    await race.StoreMessage()
                break

    return FullMessagetoSend, UUID, RaceObject

async def DetermineVolunteerReqs(row):
    if "Qual" in row.Race + row.Round:
            VolunteerMinimum = 4
    else:
        VolunteerMinimum = 2
    CommentatorText = row[KeyColumnIndexes[2]]
    Commentators, Trackers = [], []
    if not pandas.isna(CommentatorText):
        Commentators = CommentatorText.replace(" ", "").split(",")
    TrackerText = row[KeyColumnIndexes[3]]
    if not pandas.isna(TrackerText):
        Trackers = TrackerText.replace(" ", "").split(",")
    DoubleDuty = list(set(Commentators) & set(Trackers))
    RequiredCommentators = 2 - len(Commentators) # we really only need 2 commentators at most
    RequiredTrackers = VolunteerMinimum - len(Trackers)
    while len(DoubleDuty) > 0:
        if len(Commentators) > len(Trackers):
            Commentators.remove(DoubleDuty[0])
        elif len(Commentators) <= len(Trackers): # less people sign up to commentate than track, so we by default just assume the commentator will drop out of tracking
            Trackers.remove(DoubleDuty[0])
        RequiredCommentators = 2 - len(Commentators)
        RequiredTrackers = VolunteerMinimum - len(Trackers)
        DoubleDuty = list(set(Commentators) & set(Trackers))
    return RequiredCommentators, RequiredTrackers

@bot.event
async def on_ready():
    Scheduler.add_job(CheckSheet, "interval", minutes = 5) # change this line if you're swapping between beta and main
    Scheduler.start()

async def EditMessage(Race: ScheduledRace, TargetText, NewPersonCount = 0):
    DiscordMessageID = Race.messageID
    TargetMessage = await bot.get_channel(WWRVolunteersChatChannelID).fetch_message(DiscordMessageID)
    OriginalMessage = TargetMessage.content
    NewMessage = await ReplaceOldMessage(OriginalMessage, TargetText, NewPersonCount)
    await TargetMessage.edit(content = NewMessage)
    await UpdateStoredMessageFile(Race)

async def ReplaceOldMessage(OriginalMessage, PersonType, NewPersonCount):
    PersonRegexPattern = rf"(?:~~(\d+)~~\s*)?(\d+)\s+({PersonType}?)"
    PeriodRegexPattern = r"\.(?=\n)"

    def ReplaceVolunteerNum(match):
        StruckthroughNumber = match.group(1)       # old struck number (if exists)
        CurrentNum = int(match.group(2))     # current visible number
        TargetWord = match.group(3)         # tracker(s) or commentator(s)

        return f"~~{CurrentNum}~~ {NewPersonCount} {TargetWord}"

    NewMessage = re.sub(PersonRegexPattern, ReplaceVolunteerNum, OriginalMessage, flags=re.IGNORECASE)

    if PersonType == "commentator":
        NewRole = CommentatorRoleID
    elif PersonType == "tracker":
        NewRole = TrackerRoleID

    if NewRole not in NewMessage:
        NewMessage = NewRole + " " + NewMessage
        NewMessage = re.sub(PeriodRegexPattern, f" and {NewPersonCount} {PersonType}(s).", NewMessage)
        print(NewMessage)

    return NewMessage

async def UpdateStoredMessageFile(Race: ScheduledRace):
    with open(StoredMessagesFile, "r+", encoding = "utf-8") as file:
        rows = file.read().splitlines()
        for row in rows:
            if row.split(", ")[5] == f"{Race.messageID}]":
                rows.remove(row)
    with open(StoredMessagesFile, "w", encoding = "utf-8") as file:
        if rows == []:
            file.write("")
        else:
            file.write("\n".join(rows))
            file.write("\n")

async def TransmitMessage(DiscordMessage, ChannelID):
    channel = bot.get_channel(ChannelID)
    message = await channel.send(content = DiscordMessage)
    return message

async def TransmitError(ErrorMessage):
    channel = bot.get_channel(ErrorLogChannelID)
    await channel.send(content = ErrorMessage)

async def RefreshSheet(): # refreshes the sheet so that formulas have a chance to calculate before we get the values
    global client

    if client is None:
        scope = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = service_account.Credentials.from_service_account_file(ServiceAcc, scopes = scope)
        client = gspread.authorize(credentials)
    
    sheet = client.open_by_key(APITargetSheet)

    specificworksheet = sheet.worksheet("Volunteer Signups")

    kfiveget = specificworksheet.acell("K5").value or ""

    specificworksheet.update(range_name = "K5", values = [[kfiveget]])

    await asyncio.sleep(5)

bot.run(token)