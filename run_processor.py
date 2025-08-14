#!/usr/bin/env python3
"""
Simple script to run the SEC filing processor
"""

from src.processor import FilingProcessor


def main():
    """Run the filing processor"""
    processor = FilingProcessor()
    signals = processor.run_pipeline()
    
    print(f"\nGenerated {len(signals)} signals:")
    for signal in signals:
        print(f"- {signal['cik']}: {signal['form_type']} -> {signal['signal_type']}")


if __name__ == "__main__":
    main()
