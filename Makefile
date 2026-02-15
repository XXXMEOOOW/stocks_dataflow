.PHONY: format

format:
	@echo "Formatting new or modified Python files..."
	@files=$$( \
		( \
			git diff --name-only --diff-filter=ACM && \
			git diff --cached --name-only --diff-filter=ACM && \
			git ls-files --others --exclude-standard \
		) | sort -u | grep -E '\.py$$' || true \
	); \
	if [ -n "$$files" ]; then \
		echo "$$files" | xargs poetry run isort; \
		echo "$$files" | xargs poetry run black; \
	else \
		echo "No new or modified Python files to format."; \
	fi
