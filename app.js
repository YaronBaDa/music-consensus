// Consensus Frontend
let allAlbums = [];
let filteredAlbums = [];

// Score helpers
function getScoreClass(score) {
  if (score >= 90) return 'score-excellent';
  if (score >= 80) return 'score-good';
  if (score >= 70) return 'score-solid';
  if (score >= 60) return 'score-mixed';
  return 'score-poor';
}

function getConsensusScore(album) {
  const scores = [];
  if (album.aoty_critic != null) scores.push(album.aoty_critic);
  if (album.aoty_user != null) scores.push(album.aoty_user);
  if (album.metacritic != null) scores.push(album.metacritic);
  if (album.rym != null) scores.push(album.rym);
  if (album.discogs != null) scores.push(album.discogs);
  if (album.musicbrainz != null) scores.push(album.musicbrainz);
  if (scores.length === 0) return 0;
  return Math.round(scores.reduce((a, b) => a + b, 0) / scores.length);
}

// Load data
async function loadData() {
  try {
    const res = await fetch('data.json?v=' + Date.now());
    allAlbums = await res.json();
    // Pre-compute consensus scores
    allAlbums.forEach(a => {
      a.consensus = getConsensusScore(a);
    });
    populateYearFilter();
    populateGenreFilter();
    updateStats();
    // Default to 2026
    document.getElementById('yearFilter').value = '2026';
    applyFilters();
    document.getElementById('loadingState').style.display = 'none';
  } catch (e) {
    console.error('Failed to load data:', e);
    document.getElementById('loadingState').innerHTML = '<p>Failed to load album data.</p>';
  }
}

function updateStats() {
  document.getElementById('albumCount').textContent = allAlbums.length;
  const sources = new Set(allAlbums.map(a => a.source).filter(Boolean));
  document.getElementById('sourceCount').textContent = sources.size;
}

function populateYearFilter() {
  const years = new Set();
  allAlbums.forEach(a => { if (a.year) years.add(a.year); });
  const select = document.getElementById('yearFilter');
  // Keep the "All Years" option, add year options descending
  Array.from(years).sort((a, b) => b - a).forEach(y => {
    const opt = document.createElement('option');
    opt.value = String(y);
    opt.textContent = y;
    select.appendChild(opt);
  });
}

function populateGenreFilter() {
  const genres = new Set();
  allAlbums.forEach(a => { if (a.genre) genres.add(a.genre); });
  const select = document.getElementById('genreFilter');
  Array.from(genres).sort().forEach(g => {
    const opt = document.createElement('option');
    opt.value = g;
    opt.textContent = g;
    select.appendChild(opt);
  });
}

// Render
function renderAlbums(albums) {
  const grid = document.getElementById('albumsGrid');
  const empty = document.getElementById('emptyState');

  if (albums.length === 0) {
    grid.innerHTML = '';
    empty.style.display = 'block';
    return;
  }

  empty.style.display = 'none';

  // Determine rank based on current sort
  const sortedForRank = [...albums].sort((a, b) => b.consensus - a.consensus);
  const rankMap = new Map(sortedForRank.map((a, i) => [a.id || (a.artist + '|||' + a.album), i + 1]));

  grid.innerHTML = albums.map((album, idx) => {
    const id = album.id || (album.artist + '|||' + album.album);
    const rank = rankMap.get(id);
    const scoreClass = getScoreClass(album.consensus);
    const cover = album.cover || 'https://via.placeholder.com/300x300/1a1a2e/6b6b8a?text=No+Cover';

    const sourcePills = [
      { key: 'metacritic', label: 'M', color: '#4ecdc4' },
      { key: 'discogs', label: 'D', color: '#a78bfa' },
      { key: 'musicbrainz', label: 'MB', color: '#f472b6' },
      { key: 'aoty_critic', label: 'A', color: '#ff6b6b' },
      { key: 'aoty_user', label: 'AU', color: '#ff8e8e' },
      { key: 'rym', label: 'R', color: '#ffe66d' },
    ].map(s => {
      const val = album[s.key];
      if (val == null) return '';
      return `<span class="source-pill" style="background:${s.color}20;color:${s.color};border-color:${s.color}40">${s.label} ${val}</span>`;
    }).join('');

    return `
      <article class="album-card" onclick="openModal(${idx})">
        <div class="album-cover-wrap">
          <span class="album-rank">${rank}</span>
          <span class="album-consensus ${scoreClass}">${album.consensus}</span>
          <img class="album-cover" src="${cover}" alt="${album.album}" loading="lazy">
        </div>
        <div class="album-info">
          <div class="album-title">${album.album}</div>
          <div class="album-artist">${album.artist}</div>
          <div class="album-meta">
            <span>${album.year}</span>
            ${album.genre ? `<span class="album-genre">${album.genre}</span>` : ''}
          </div>
          ${sourcePills ? `<div class="album-sources">${sourcePills}</div>` : ''}
        </div>
      </article>
    `;
  }).join('');
}

// Filters
function applyFilters() {
  const search = document.getElementById('searchInput').value.toLowerCase();
  const year = document.getElementById('yearFilter').value;
  const genre = document.getElementById('genreFilter').value;
  const sort = document.getElementById('sortSelect').value;

  filteredAlbums = allAlbums.filter(a => {
    if (search && !a.album.toLowerCase().includes(search) && !a.artist.toLowerCase().includes(search)) return false;
    if (year && String(a.year) !== year) return false;
    if (genre && a.genre !== genre) return false;
    return true;
  });

  // Sort
  filteredAlbums.sort((a, b) => {
    switch (sort) {
      case 'consensus': return b.consensus - a.consensus;
      case 'year': return b.year - a.year;
      case 'artist': return a.artist.localeCompare(b.artist);
      case 'album': return a.album.localeCompare(b.album);
      case 'metacritic': return (b.metacritic || 0) - (a.metacritic || 0);
      default: return 0;
    }
  });

  renderAlbums(filteredAlbums);

  const hasFilters = search || year || genre;
  document.getElementById('clearFilters').style.display = hasFilters ? 'block' : 'none';
}

// Modal
function openModal(index) {
  const album = filteredAlbums[index];
  const body = document.getElementById('modalBody');
  const cover = album.cover || 'https://via.placeholder.com/300x300/1a1a2e/6b6b8a?text=No+Cover';
  const scoreClass = getScoreClass(album.consensus);

  const sources = [
    { name: 'AOTY Critic', key: 'aoty_critic', color: '#ff6b6b' },
    { name: 'AOTY User', key: 'aoty_user', color: '#ff8e8e' },
    { name: 'Metacritic', key: 'metacritic', color: '#4ecdc4' },
    { name: 'RateYourMusic', key: 'rym', color: '#ffe66d' },
    { name: 'Discogs', key: 'discogs', color: '#a78bfa' },
    { name: 'MusicBrainz', key: 'musicbrainz', color: '#f472b6' },
  ];

  const breakdown = sources.map(s => {
    const val = album[s.key];
    if (val == null) return '';
    return `
      <div class="source-row">
        <div class="source-name">${s.name}</div>
        <div class="source-bar-bg">
          <div class="source-bar" style="width:${val}%; background:${s.color}"></div>
        </div>
        <div class="source-score">${val}</div>
      </div>
    `;
  }).join('');

  body.innerHTML = `
    <div class="modal-header">
      <img class="modal-cover" src="${cover}" alt="${album.album}">
      <div class="modal-info">
        <h2>${album.album}</h2>
        <div class="artist">${album.artist}</div>
        <div class="year-genre">${album.year}${album.genre ? ' · ' + album.genre : ''}</div>
        <div class="consensus-big ${scoreClass}">
          <span>${album.consensus}</span>
          <span style="font-size:0.7rem; opacity:0.8">/100</span>
        </div>
      </div>
    </div>
    <div class="score-breakdown">
      <h3>Score Breakdown</h3>
      ${breakdown}
    </div>
  `;

  document.getElementById('modalOverlay').classList.add('active');
  document.body.style.overflow = 'hidden';
}

function closeModal() {
  document.getElementById('modalOverlay').classList.remove('active');
  document.body.style.overflow = '';
}

// Event listeners
document.getElementById('searchInput').addEventListener('input', applyFilters);
document.getElementById('yearFilter').addEventListener('change', applyFilters);
document.getElementById('genreFilter').addEventListener('change', applyFilters);
document.getElementById('sortSelect').addEventListener('change', applyFilters);
document.getElementById('clearFilters').addEventListener('click', () => {
  document.getElementById('searchInput').value = '';
  document.getElementById('yearFilter').value = '2026';
  document.getElementById('genreFilter').value = '';
  applyFilters();
});

document.getElementById('modalClose').addEventListener('click', closeModal);
document.getElementById('modalOverlay').addEventListener('click', e => {
  if (e.target === document.getElementById('modalOverlay')) closeModal();
});

document.addEventListener('keydown', e => {
  if (e.key === 'Escape') closeModal();
});

// Init
loadData();
