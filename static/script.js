/**
 * SQL Query Analyzer v2.0 — Frontend Logic
 * ==========================================
 * Handles: analysis requests, schema loading, database switching,
 * file upload, sample queries, and result rendering.
 */

// ═══════════════════════════════════════════════════════════
// DOM References
// ═══════════════════════════════════════════════════════════
const queryInput      = document.getElementById('query-input');
const analyzeBtn      = document.getElementById('analyze-btn');
const spinner         = document.getElementById('spinner');
const errorBox        = document.getElementById('error-box');
const errorMessage    = document.getElementById('error-message');
const resultsSection  = document.getElementById('results');
const samplesList     = document.getElementById('samples-list');
const schemaContainer = document.getElementById('schema-container');
const dbSelect        = document.getElementById('db-select');
const dbBadgeName     = document.getElementById('db-badge-name');
const uploadInput     = document.getElementById('upload-input');
const uploadStatus    = document.getElementById('upload-status');
const uploadArea      = document.getElementById('upload-area');


// ═══════════════════════════════════════════════════════════
// Initialization — runs on page load
// ═══════════════════════════════════════════════════════════
document.addEventListener('DOMContentLoaded', () => {
    loadSchema();
    loadSamples();
    loadUploadedDbs();
    setupDragDrop();
});


// ═══════════════════════════════════════════════════════════
// 1. Schema Viewer
// ═══════════════════════════════════════════════════════════

/**
 * Fetch and render the schema of the currently active database.
 */
async function loadSchema() {
    try {
        const res = await fetch('/schema');
        const data = await res.json();
        renderSchema(data.schema);
        dbBadgeName.textContent = data.db_name;
    } catch (e) {
        schemaContainer.innerHTML = '<div class="schema-empty">Failed to load schema.</div>';
        console.error('Schema load error:', e);
    }
}

/**
 * Render the schema tables into the sidebar.
 */
function renderSchema(schema) {
    if (!schema || schema.length === 0 || schema[0]?.error) {
        schemaContainer.innerHTML = '<div class="schema-empty">No tables found.</div>';
        return;
    }

    let html = '';
    schema.forEach(table => {
        html += `<div class="schema-table">`;
        html += `<div class="schema-table__name">${table.table}</div>`;
        html += `<div class="schema-table__cols">`;

        table.columns.forEach(col => {
            html += `<div class="schema-col">`;
            html += `<span class="schema-col__name">${col.name}</span>`;
            html += `<span class="schema-col__type">${col.type}</span>`;
            if (col.pk) html += `<span class="schema-col__badge schema-col__badge--pk">PK</span>`;
            if (col.notnull) html += `<span class="schema-col__badge schema-col__badge--nn">NN</span>`;
            html += `</div>`;
        });

        // Show indexes if any
        if (table.indexes && table.indexes.length > 0) {
            table.indexes.forEach(idx => {
                html += `<div class="schema-index">idx: ${idx.name} (${idx.columns.join(', ')})${idx.unique ? ' UNIQUE' : ''}</div>`;
            });
        }

        html += `</div></div>`;
    });

    schemaContainer.innerHTML = html;
}


// ═══════════════════════════════════════════════════════════
// 2. Database Upload
// ═══════════════════════════════════════════════════════════

/**
 * Handle file upload to the server.
 */
async function uploadFile(input) {
    const file = input.files[0];
    if (!file) return;

    // Client-side validation
    const ext = file.name.split('.').pop().toLowerCase();
    const allowed = ['db', 'sqlite', 'sqlite3'];
    if (!allowed.includes(ext)) {
        showUploadStatus('error', `Invalid file type .${ext}. Use .db, .sqlite, or .sqlite3.`);
        input.value = '';
        return;
    }

    if (file.size > 16 * 1024 * 1024) {
        showUploadStatus('error', 'File too large. Maximum is 16 MB.');
        input.value = '';
        return;
    }

    showUploadStatus('info', 'Uploading...');

    const formData = new FormData();
    formData.append('file', file);

    try {
        const res = await fetch('/upload', { method: 'POST', body: formData });
        const data = await res.json();

        if (data.success) {
            showUploadStatus('success', `Uploaded "${data.db_name}" successfully!`);
            renderSchema(data.schema);
            dbBadgeName.textContent = data.db_name;
            // Add to dropdown if not already there
            addDbOption(data.db_name);
            dbSelect.value = data.db_name;
            // Clear old results
            resultsSection.classList.remove('visible');
        } else {
            showUploadStatus('error', data.error || 'Upload failed.');
        }
    } catch (e) {
        showUploadStatus('error', 'Network error during upload.');
        console.error(e);
    }

    input.value = '';
}

/**
 * Set up drag-and-drop on the upload area.
 */
function setupDragDrop() {
    uploadArea.addEventListener('dragover', (e) => {
        e.preventDefault();
        uploadArea.classList.add('drag-over');
    });
    uploadArea.addEventListener('dragleave', () => {
        uploadArea.classList.remove('drag-over');
    });
    uploadArea.addEventListener('drop', (e) => {
        e.preventDefault();
        uploadArea.classList.remove('drag-over');
        if (e.dataTransfer.files.length > 0) {
            uploadInput.files = e.dataTransfer.files;
            uploadFile(uploadInput);
        }
    });
}

/**
 * Show upload status message.
 */
function showUploadStatus(type, message) {
    uploadStatus.textContent = message;
    uploadStatus.className = 'upload-status visible';
    if (type === 'success') uploadStatus.classList.add('upload-status--success');
    else if (type === 'error') uploadStatus.classList.add('upload-status--error');
    else uploadStatus.style.color = 'var(--text-secondary)';

    // Auto-hide after 5 seconds
    setTimeout(() => {
        uploadStatus.classList.remove('visible');
    }, 5000);
}


// ═══════════════════════════════════════════════════════════
// 3. Database Switching
// ═══════════════════════════════════════════════════════════

/**
 * Load the list of uploaded databases into the dropdown.
 */
async function loadUploadedDbs() {
    try {
        const res = await fetch('/uploaded-dbs');
        const data = await res.json();

        data.databases.forEach(db => addDbOption(db));

        // Set current selection
        if (data.current === 'Default Sample Database') {
            dbSelect.value = 'default';
        } else {
            dbSelect.value = data.current;
        }
    } catch (e) {
        console.error('Failed to load uploaded DBs:', e);
    }
}

/**
 * Add a database option to the dropdown (if not already present).
 */
function addDbOption(name) {
    for (let opt of dbSelect.options) {
        if (opt.value === name) return; // Already exists
    }
    const option = document.createElement('option');
    option.value = name;
    option.textContent = name;
    dbSelect.appendChild(option);
}

/**
 * Switch the active database (triggered by dropdown change).
 */
async function switchDatabase() {
    const selected = dbSelect.value;

    try {
        const res = await fetch('/switch-db', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ database: selected }),
        });
        const data = await res.json();

        if (data.success) {
            renderSchema(data.schema);
            dbBadgeName.textContent = data.db_name;
            resultsSection.classList.remove('visible');
            hideError();
        } else {
            showError(data.error || 'Failed to switch database.');
        }
    } catch (e) {
        showError('Network error while switching database.');
        console.error(e);
    }
}

/**
 * Reset to default database.
 */
async function resetDatabase() {
    try {
        const res = await fetch('/reset-db', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
        });
        const data = await res.json();

        if (data.success) {
            renderSchema(data.schema);
            dbBadgeName.textContent = data.db_name;
            dbSelect.value = 'default';
            resultsSection.classList.remove('visible');
            hideError();
            showUploadStatus('success', 'Reset to default database.');
        }
    } catch (e) {
        showError('Failed to reset database.');
        console.error(e);
    }
}


// ═══════════════════════════════════════════════════════════
// 4. Sample Queries
// ═══════════════════════════════════════════════════════════

/**
 * Load sample queries from the backend.
 */
async function loadSamples() {
    try {
        const res = await fetch('/sample-queries');
        const samples = await res.json();

        samplesList.innerHTML = '';
        samples.forEach(sample => {
            const btn = document.createElement('button');
            btn.className = 'sample-item';
            btn.textContent = sample.label;
            btn.onclick = () => {
                queryInput.value = sample.query;
                samplesList.classList.remove('open');
                hideError();
                resultsSection.classList.remove('visible');
                queryInput.focus();
            };
            samplesList.appendChild(btn);
        });
    } catch (e) {
        console.error('Failed to load samples:', e);
    }
}

/**
 * Toggle sample queries dropdown.
 */
function toggleSamples() {
    samplesList.classList.toggle('open');
}

// Close dropdown when clicking outside
document.addEventListener('click', (e) => {
    if (!e.target.closest('.samples-dropdown')) {
        samplesList.classList.remove('open');
    }
});


// ═══════════════════════════════════════════════════════════
// 5. Query Analysis (Main Action)
// ═══════════════════════════════════════════════════════════

/**
 * Send query to backend for analysis and render all results.
 */
async function analyzeQuery() {
    const query = queryInput.value.trim();

    if (!query) {
        showError('Please enter a SQL query before analyzing.');
        return;
    }

    hideError();
    resultsSection.classList.remove('visible');
    setLoading(true);

    try {
        const res = await fetch('/analyze', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ query }),
        });

        const data = await res.json();

        if (!data.valid) {
            showError(data.error || 'Invalid query.');
            setLoading(false);
            return;
        }

        // Render all result sections
        document.getElementById('summary-content').textContent = data.summary;
        renderComplexity(data.complexity);
        renderList('issues', data.analysis.issues);
        renderList('warnings', data.analysis.warnings);
        renderList('suggestions', data.analysis.suggestions);
        renderIndexSuggestion(data.index_suggestion);
        renderPlan(data.plan);
        renderOptimized(data.optimized);
        renderComparison(data.comparison);

        resultsSection.classList.add('visible');

        setTimeout(() => {
            resultsSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }, 100);

    } catch (err) {
        showError('Network error — could not reach the server.');
        console.error(err);
    } finally {
        setLoading(false);
    }
}


// ═══════════════════════════════════════════════════════════
// 6. Result Renderers
// ═══════════════════════════════════════════════════════════

/**
 * Render the enhanced complexity score card with breakdown.
 */
function renderComplexity(complexity) {
    const circle = document.getElementById('complexity-circle');
    const scoreNum = document.getElementById('complexity-score-number');
    const levelText = document.getElementById('complexity-level-text');
    const descText = document.getElementById('complexity-desc');
    const breakdownEl = document.getElementById('complexity-breakdown');

    // Score circle
    scoreNum.textContent = complexity.score;
    circle.style.color = complexity.color;
    circle.style.borderColor = complexity.color;
    circle.style.boxShadow = `0 0 20px ${hexToRgba(complexity.color, 0.25)}, inset 0 0 12px ${hexToRgba(complexity.color, 0.08)}`;

    // Level and description
    levelText.textContent = `${complexity.emoji} ${complexity.level}`;
    levelText.style.color = complexity.color;
    descText.textContent = complexity.description;

    // Breakdown
    breakdownEl.innerHTML = '';
    if (complexity.breakdown && complexity.breakdown.length > 0) {
        complexity.breakdown.forEach(item => {
            const div = document.createElement('div');
            div.className = 'complexity-breakdown-item';
            div.innerHTML = `
                <span class="complexity-breakdown-item__rule">${item.rule}</span>
                <span class="complexity-breakdown-item__detail">${item.detail}</span>
                <span class="complexity-breakdown-item__points" style="background:${hexToRgba(complexity.color, 0.12)};color:${complexity.color}">+${item.points}</span>
            `;
            breakdownEl.appendChild(div);
        });
    } else {
        breakdownEl.innerHTML = '<div class="empty-state">No complexity penalties — great query!</div>';
    }
}

/**
 * Render smart index recommendation section.
 */
function renderIndexSuggestion(indexData) {
    const container = document.getElementById('index-content');
    container.innerHTML = '';

    if (!indexData.has_suggestions) {
        container.innerHTML = '<div class="index-no-suggestion">✅ No index issues detected — query uses indexes efficiently!</div>';
        return;
    }

    indexData.suggestions.forEach(item => {
        const div = document.createElement('div');
        div.className = 'index-suggestion-item';
        if (item.sql) {
            div.innerHTML = `
                <div class="index-suggestion-sql">✅ Suggested Index: <code>${item.sql}</code></div>
                <div class="index-suggestion-reason">${item.reason}</div>
            `;
        } else {
            div.innerHTML = `
                <div class="index-suggestion-sql">ℹ️ ${item.reason}</div>
            `;
        }
        container.appendChild(div);
    });
}

/**
 * Render performance comparison between original and optimized query.
 */
function renderComparison(comparison) {
    const origEl = document.getElementById('comparison-original');
    const optEl = document.getElementById('comparison-optimized');
    const verdictEl = document.getElementById('comparison-verdict');

    origEl.innerHTML = '';
    optEl.innerHTML = '';

    // If comparison was skipped (no valid optimized query), show status message
    if (comparison.skipped) {
        origEl.innerHTML = '<div class="empty-state">ℹ️ Comparison skipped — no optimization was applied</div>';
        optEl.innerHTML = '<div class="empty-state">ℹ️ Comparison skipped — no optimization was applied</div>';
        verdictEl.className = 'comparison-verdict comparison-verdict--same';
        verdictEl.innerHTML = `<span>ℹ️</span> <span>${comparison.improvements.join(', ')}</span>`;
        return;
    }

    // Render original plan lines
    comparison.original.forEach(item => {
        const div = document.createElement('div');
        div.className = `comparison-line comparison-line--${item.type}`;
        div.innerHTML = `<span>${item.icon}</span> <span>${item.text}</span> <span style="opacity:0.7;font-size:0.72rem">(${item.label})</span>`;
        origEl.appendChild(div);
    });
    if (comparison.original.length === 0) {
        origEl.innerHTML = '<div class="empty-state">No plan data</div>';
    }

    // Render optimized plan lines
    comparison.optimized.forEach(item => {
        const div = document.createElement('div');
        div.className = `comparison-line comparison-line--${item.type}`;
        div.innerHTML = `<span>${item.icon}</span> <span>${item.text}</span> <span style="opacity:0.7;font-size:0.72rem">(${item.label})</span>`;
        optEl.appendChild(div);
    });
    if (comparison.optimized.length === 0) {
        optEl.innerHTML = '<div class="empty-state">No plan data</div>';
    }

    // Verdict
    if (comparison.improved) {
        verdictEl.className = 'comparison-verdict comparison-verdict--improved';
        verdictEl.innerHTML = `<span>✔</span> <span>Performance Improved — ${comparison.improvements.join(', ')}</span>`;
    } else {
        verdictEl.className = 'comparison-verdict comparison-verdict--same';
        verdictEl.innerHTML = `<span>ℹ️</span> <span>${comparison.improvements.join(', ')}</span>`;
    }
}

/**
 * Render a result list (issues, warnings, or suggestions).
 */
function renderList(type, items) {
    const list  = document.getElementById(`${type}-list`);
    const count = document.getElementById(`${type}-count`);

    count.textContent = items.length;
    list.innerHTML = '';

    if (items.length === 0) {
        list.innerHTML = `<li class="empty-state">No ${type} detected!</li>`;
        return;
    }

    items.forEach(item => {
        const li = document.createElement('li');
        li.className = 'result-item';
        li.innerHTML = `<span class="result-item__marker"></span><span>${item}</span>`;
        list.appendChild(li);
    });
}

/**
 * Render execution plan (raw + interpreted).
 */
function renderPlan(plan) {
    document.getElementById('plan-raw').textContent =
        plan.raw.length > 0 ? plan.raw.join('\n') : 'No plan data available.';

    const interpEl = document.getElementById('plan-interpreted');
    interpEl.innerHTML = '';

    if (plan.interpretation.length === 0) {
        interpEl.innerHTML = '<div class="empty-state">No interpretation available.</div>';
        return;
    }

    plan.interpretation.forEach(item => {
        const div = document.createElement('div');
        div.className = `plan-interpreted-item plan-interpreted-item--${item.type}`;
        div.innerHTML = `<span>${item.message}</span>`;
        interpEl.appendChild(div);
    });
}

/**
 * Render the optimized query section.
 * Handles three statuses from the backend:
 *   - "optimized"        → show the improved query + list of changes
 *   - "already_optimal"  → show a reassuring "already optimal" message
 *   - "skipped"          → warn that optimization was skipped for safety
 */
function renderOptimized(optimized) {
    document.getElementById('optimized-query-text').textContent = optimized.optimized;

    const changesList = document.getElementById('optimized-changes');
    changesList.innerHTML = '';

    // Show status message when no changes were made
    if (optimized.changes.length === 0) {
        const status = optimized.status || 'already_optimal';
        const statusMessage = optimized.status_message || 'No optimizations needed — query looks good!';

        let icon = 'ℹ️';
        let cssClass = 'empty-state';
        if (status === 'already_optimal') {
            icon = '✅';
        } else if (status === 'skipped') {
            icon = '⚠️';
            cssClass = 'empty-state empty-state--warning';
        }

        changesList.innerHTML = `<li class="${cssClass}">${icon} ${statusMessage}</li>`;
        return;
    }

    optimized.changes.forEach(change => {
        const li = document.createElement('li');
        li.className = 'optimized-change-item';
        li.innerHTML = `<span>${change}</span>`;
        changesList.appendChild(li);
    });
}

/**
 * Copy the optimized query to clipboard.
 */
function copyOptimized() {
    const text = document.getElementById('optimized-query-text').textContent;
    navigator.clipboard.writeText(text).then(() => {
        const btn = document.querySelector('.copy-btn');
        btn.textContent = 'Copied!';
        setTimeout(() => { btn.textContent = 'Copy'; }, 1500);
    }).catch(err => {
        console.error('Copy failed:', err);
    });
}


// ═══════════════════════════════════════════════════════════
// 7. Utility Functions
// ═══════════════════════════════════════════════════════════

function showError(msg) {
    errorMessage.innerHTML = msg;
    errorBox.classList.add('visible');
}

function hideError() {
    errorBox.classList.remove('visible');
}

function setLoading(loading) {
    analyzeBtn.disabled = loading;
    analyzeBtn.classList.toggle('btn--loading', loading);
}

/**
 * Hex color -> rgba string.
 */
function hexToRgba(hex, alpha) {
    const r = parseInt(hex.slice(1, 3), 16);
    const g = parseInt(hex.slice(3, 5), 16);
    const b = parseInt(hex.slice(5, 7), 16);
    return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

// Keyboard shortcut: Ctrl+Enter to analyze
queryInput.addEventListener('keydown', (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        e.preventDefault();
        analyzeQuery();
    }
});
