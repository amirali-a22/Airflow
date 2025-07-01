# generate_env_sample.py

with open('.env', 'r') as infile, open('.env.sample', 'w') as outfile:
    for line in infile:
        if '=' in line and not line.strip().startswith('#'):
            key = line.split('=')[0].strip()
            outfile.write(f"{key}=\n")
        else:
            outfile.write(line)
