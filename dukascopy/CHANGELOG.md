## [0.2.0-stable] - 2025-12-03

### Changed
- **Repository curation**: History cleaned to showcase production-ready architecture
- **Calendar alignment**: Weekly candles now match Dukascopy charts exactly  
- **Documentation**: Enhanced README with clearer setup, usage, and troubleshooting

### For Existing Users
If you cloned this repository before 2025-12-03:

```bash
# Update to the cleaned version
cd /path/to/repo
git fetch origin
git reset --hard origin/main

# Rebuild data to apply calendar alignment fixes
./rebuild-weekly.sh
```

## [0.3.0-stable] - 2025-12-10

### Changed
- **Configuration**: Everything is now configurable through YAML
- **Documentation**: Updated README with instructions