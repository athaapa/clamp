Output demo/clamp_demo.gif

# Window appearance
Set WindowBar Colorful
Set WindowBarSize 40

# Theme - Catppuccin Mocha (soft, popular)
Set Theme "Catppuccin Mocha"

# Terminal size
Set FontSize 28
Set Width 1400
Set Height 700
Set Padding 20

# Default typing speed
Set TypingSpeed 50ms
Set Shell "bash"

# --- HIDDEN SETUP ---
Hide
Type "source .venv/bin/activate"
Enter
Sleep 500ms
Type "rm -rf .clamp docs_v1.txt docs_v2.txt my_product"
Enter
Sleep 500ms
Type "docker start qdrant_demo 2>/dev/null || docker run -d -p 6333:6333 --name qdrant_demo qdrant/qdrant"
Enter
Sleep 2s
Type "export PS1='$ '"
Enter
Type "clear"
Enter
Show

# --- DEMO ---

# Narration: Initialize
Type "# Initialize Clamp version control"
Enter
Sleep 500ms
Type "clamp init"
Enter
Sleep 1.5s

# Narration: Create first version
Type "# Create initial policy document"
Enter
Sleep 500ms
Set TypingSpeed 40ms
Type "echo 'Refund policy: 30-day returns allowed' > docs_v1.txt"
Enter
Sleep 500ms

# Commit v1 (important - slower)
Set TypingSpeed 60ms
Type "clamp commit docs_v1.txt policies 'v1: Initial policy'"
Enter
Sleep 1.5s

# Check status
Type "clamp status policies"
Enter
Sleep 2s

# Narration: The problem
Type "# Someone pushes a bad update..."
Enter
Sleep 800ms
Set TypingSpeed 40ms
Type "echo 'Refund policy: NO REFUNDS ALLOWED' > docs_v2.txt"
Enter
Sleep 500ms

Set TypingSpeed 60ms
Type "clamp commit docs_v2.txt policies 'v2: Policy update'"
Enter
Sleep 1.5s

# Show history
Type "clamp history policies"
Enter
Sleep 2.5s

# Narration: The fix (dramatic, slow)
Type "# Production is broken! Checking out previous version..."
Enter
Sleep 1s

Set TypingSpeed 100ms
Type "clamp checkout policies HEAD~1 -f"
Enter
Sleep 2s

# Verify (fast)
Set TypingSpeed 50ms
Type "clamp status policies"
Enter
Sleep 2s

Type "clamp history policies"
Enter
Sleep 3s

# Narration: Success
Type "# Instant checkout. Crisis averted."
Enter
Sleep 2s