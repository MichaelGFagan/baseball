import random

batter = {'o_swing': .229, 'z_swing': .579, 'o_contact': .692, 'z_contact': .88}
pitcher = {'o_swing': .355, 'z_swing': .708, 'o_contact': .581, 'z_contact': .811, 'zone': .44}
environment = {
    'o_swing': (batter['o_swing'] + pitcher['o_swing']) / 2,
    'z_swing': (batter['z_swing'] + pitcher['z_swing']) / 2,
    'o_contact': (batter['o_contact'] + pitcher['o_contact']) / 2,
    'z_contact': (batter['z_contact'] + pitcher['z_contact']) / 2,
    'zone': pitcher['zone']
}

pitch = 0
zone = 0
o_swing = 0
z_swing = 0
o_contact = 0
z_contact = 0

stats = [0, 0, 0, 0, 0, 0]

def pitch_sim():
    stats =[1, 0, 0, 0, 0, 0]
    in_zone = False
    if random.uniform(0, 1) < environment['zone']:
        in_zone = True

    if in_zone:
        stats[1] += 1
        if random.uniform(0, 1) < environment['z_swing']:
            stats[3] += 1
            if random.uniform(0, 1) < environment['z_contact']:
                stats[5] += 1
        if random.uniform(0, 1) < environment['o_swing']:
            stats[2] += 1
            if random.uniform(0, 1) < environment['o_contact']:
                stats[4] += 1

    return stats

for i in range(0, 100000):
    stat_pitch = pitch_sim()
    for i in range(0, len(stat_pitch)):
        stats[i] += stat_pitch[i]

print('Pitches: ' + str(stats[0]))
print('Pitches in zone: ' + str(stats[1]))
print('O-Swing: ' + str(stats[2]))
print('Z-Swing: ' + str(stats[3]))
print('O-Contact: ' + str(stats[4]))
print('Z-Contact: ' + str(stats[5]))
