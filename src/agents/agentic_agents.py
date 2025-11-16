"""
Agentic Data Quality System
- Data Profiler Agent: Auto-discover data characteristics and issues
- Quality Agent: Monitor and score data quality
- Remediation Agent: Auto-fix common data issues
"""

import polars as pl
import pandas as pd
from typing import Dict, List, Tuple, Any
from loguru import logger
from dataclasses import dataclass
import json


@dataclass
class DataQualityReport:
    """Data quality assessment report"""
    table_name: str
    total_rows: int
    total_columns: int
    quality_score: float
    issues: List[Dict[str, Any]]
    recommendations: List[str]
    timestamp: str


class DataProfilerAgent:
    """
    Autonomous agent that profiles datasets and discovers quality issues
    """
    
    def __init__(self):
        self.profile_cache = {}
    
    def profile_dataset(self, df: pl.DataFrame, dataset_name: str) -> Dict[str, Any]:
        """
        Automatically profile a dataset and detect issues
        
        Returns:
            Comprehensive data profile with statistics and issues
        """
        logger.info(f"🔍 Profiling dataset: {dataset_name}")
        
        profile = {
            'dataset_name': dataset_name,
            'row_count': len(df),
            'column_count': len(df.columns),
            'memory_usage_mb': df.estimated_size('mb'),
            'columns': {},
            'data_types': {},
            'issues_detected': []
        }
        
        # Profile each column
        for col in df.columns:
            col_profile = self._profile_column(df, col)
            profile['columns'][col] = col_profile
            profile['data_types'][col] = str(df[col].dtype)
            
            # Detect column-level issues
            issues = self._detect_column_issues(df, col, col_profile)
            profile['issues_detected'].extend(issues)
        
        # Detect dataset-level issues
        dataset_issues = self._detect_dataset_issues(df)
        profile['issues_detected'].extend(dataset_issues)
        
        # Generate recommendations
        profile['recommendations'] = self._generate_recommendations(profile)
        
        # Cache profile
        self.profile_cache[dataset_name] = profile
        
        logger.info(f"✅ Profile complete: {len(profile['issues_detected'])} issues detected")
        
        return profile
    
    def _profile_column(self, df: pl.DataFrame, col: str) -> Dict[str, Any]:
        """Profile a single column"""
        col_data = df[col]
        
        profile = {
            'dtype': str(col_data.dtype),
            'null_count': col_data.null_count(),
            'null_percentage': (col_data.null_count() / len(df) * 100),
            'unique_count': col_data.n_unique(),
            'unique_percentage': (col_data.n_unique() / len(df) * 100)
        }
        
        # Numeric column stats
        if col_data.dtype in [pl.Int64, pl.Int32, pl.Float64, pl.Float32]:
            try:
                profile.update({
                    'min': float(col_data.min()),
                    'max': float(col_data.max()),
                    'mean': float(col_data.mean()),
                    'median': float(col_data.median()),
                    'std': float(col_data.std()),
                    'zeros_count': int((col_data == 0).sum()),
                    'negative_count': int((col_data < 0).sum()),
                })
            except:
                pass
        
        # String column stats
        if col_data.dtype == pl.Utf8:
            try:
                profile.update({
                    'min_length': int(col_data.str.lengths().min()),
                    'max_length': int(col_data.str.lengths().max()),
                    'avg_length': float(col_data.str.lengths().mean()),
                    'empty_strings': int((col_data == "").sum()),
                    'whitespace_issues': int(col_data.str.strip_chars().ne(col_data).sum()),
                })
            except:
                pass
        
        return profile
    
    def _detect_column_issues(self, df: pl.DataFrame, col: str, profile: Dict) -> List[Dict]:
        """Detect issues in a single column"""
        issues = []
        
        # High null percentage
        if profile['null_percentage'] > 50:
            issues.append({
                'severity': 'HIGH',
                'type': 'HIGH_NULL_RATE',
                'column': col,
                'message': f"{col} has {profile['null_percentage']:.1f}% null values",
                'recommendation': f"Consider imputation or removing column {col}"
            })
        elif profile['null_percentage'] > 10:
            issues.append({
                'severity': 'MEDIUM',
                'type': 'MODERATE_NULL_RATE',
                'column': col,
                'message': f"{col} has {profile['null_percentage']:.1f}% null values",
                'recommendation': f"Review null handling strategy for {col}"
            })
        
        # Low cardinality
        if profile['unique_percentage'] < 5 and profile['unique_count'] > 1:
            issues.append({
                'severity': 'INFO',
                'type': 'LOW_CARDINALITY',
                'column': col,
                'message': f"{col} has only {profile['unique_count']} unique values",
                'recommendation': f"Consider treating {col} as categorical"
            })
        
        # Negative values in presumably positive columns
        if 'negative_count' in profile and profile['negative_count'] > 0:
            if any(keyword in col.lower() for keyword in ['price', 'amount', 'quantity', 'age', 'count']):
                issues.append({
                    'severity': 'HIGH',
                    'type': 'NEGATIVE_VALUES',
                    'column': col,
                    'message': f"{col} has {profile['negative_count']} negative values",
                    'recommendation': f"Investigate and correct negative values in {col}"
                })
        
        # Whitespace issues
        if 'whitespace_issues' in profile and profile['whitespace_issues'] > 0:
            issues.append({
                'severity': 'LOW',
                'type': 'WHITESPACE',
                'column': col,
                'message': f"{col} has {profile['whitespace_issues']} values with leading/trailing whitespace",
                'recommendation': f"Apply string trimming to {col}"
            })
        
        return issues
    
    def _detect_dataset_issues(self, df: pl.DataFrame) -> List[Dict]:
        """Detect dataset-level issues"""
        issues = []
        
        # Check for duplicates
        duplicate_count = len(df) - df.n_unique()
        if duplicate_count > 0:
            issues.append({
                'severity': 'MEDIUM',
                'type': 'DUPLICATES',
                'column': 'ALL',
                'message': f"Found {duplicate_count} duplicate rows",
                'recommendation': "Apply deduplication strategy"
            })
        
        # Check for completely empty columns
        for col in df.columns:
            if df[col].null_count() == len(df):
                issues.append({
                    'severity': 'HIGH',
                    'type': 'EMPTY_COLUMN',
                    'column': col,
                    'message': f"Column {col} is completely empty",
                    'recommendation': f"Consider removing column {col}"
                })
        
        return issues
    
    def _generate_recommendations(self, profile: Dict) -> List[str]:
        """Generate actionable recommendations"""
        recommendations = []
        
        # Group issues by severity
        high_severity = [i for i in profile['issues_detected'] if i['severity'] == 'HIGH']
        medium_severity = [i for i in profile['issues_detected'] if i['severity'] == 'MEDIUM']
        
        if high_severity:
            recommendations.append(f"🔴 CRITICAL: Address {len(high_severity)} high-severity issues immediately")
        
        if medium_severity:
            recommendations.append(f"🟡 WARNING: Review {len(medium_severity)} medium-severity issues")
        
        # Specific recommendations
        null_issues = [i for i in profile['issues_detected'] if 'NULL' in i['type']]
        if null_issues:
            recommendations.append("Consider implementing missing value imputation strategy")
        
        dup_issues = [i for i in profile['issues_detected'] if i['type'] == 'DUPLICATES']
        if dup_issues:
            recommendations.append("Enable deduplication in Silver layer transformation")
        
        return recommendations


class QualityAgent:
    """
    Agent that monitors and scores data quality
    """
    
    def __init__(self):
        self.quality_history = []
    
    def calculate_quality_score(self, profile: Dict) -> float:
        """
        Calculate overall data quality score (0-100)
        
        Scoring factors:
        - Completeness (null rates)
        - Validity (business rule compliance)
        - Consistency (duplicates, format issues)
        - Accuracy (outliers, anomalies)
        """
        score = 100.0
        
        # Penalize for issues
        for issue in profile.get('issues_detected', []):
            if issue['severity'] == 'HIGH':
                score -= 10
            elif issue['severity'] == 'MEDIUM':
                score -= 5
            elif issue['severity'] == 'LOW':
                score -= 2
        
        # Penalize for high null rates
        for col, col_profile in profile.get('columns', {}).items():
            null_pct = col_profile.get('null_percentage', 0)
            if null_pct > 50:
                score -= 5
            elif null_pct > 20:
                score -= 2
        
        return max(0, score)
    
    def generate_quality_report(
        self, 
        df: pl.DataFrame, 
        table_name: str
    ) -> DataQualityReport:
        """Generate comprehensive quality report"""
        from datetime import datetime
        
        # Profile the data
        profiler = DataProfilerAgent()
        profile = profiler.profile_dataset(df, table_name)
        
        # Calculate score
        quality_score = self.calculate_quality_score(profile)
        
        # Create report
        report = DataQualityReport(
            table_name=table_name,
            total_rows=len(df),
            total_columns=len(df.columns),
            quality_score=quality_score,
            issues=profile['issues_detected'],
            recommendations=profile['recommendations'],
            timestamp=datetime.now().isoformat()
        )
        
        # Store history
        self.quality_history.append(report)
        
        return report


class RemediationAgent:
    """
    Agent that automatically fixes common data quality issues
    """
    
    def __init__(self):
        self.remediation_log = []
    
    def auto_remediate(
        self, 
        df: pl.DataFrame, 
        issues: List[Dict]
    ) -> Tuple[pl.DataFrame, List[Dict]]:
        """
        Automatically fix data quality issues
        
        Returns:
            (remediated_df, remediation_actions)
        """
        logger.info(f"🔧 Starting auto-remediation for {len(issues)} issues")
        
        remediation_actions = []
        df_fixed = df.clone()
        
        for issue in issues:
            action = None
            
            if issue['type'] == 'WHITESPACE':
                # Fix whitespace
                col = issue['column']
                if col in df_fixed.columns and df_fixed[col].dtype == pl.Utf8:
                    df_fixed = df_fixed.with_columns(
                        pl.col(col).str.strip_chars().alias(col)
                    )
                    action = {
                        'issue': issue,
                        'action': 'TRIMMED_WHITESPACE',
                        'column': col,
                        'status': 'SUCCESS'
                    }
            
            elif issue['type'] == 'DUPLICATES':
                # Remove duplicates
                original_count = len(df_fixed)
                df_fixed = df_fixed.unique()
                removed = original_count - len(df_fixed)
                action = {
                    'issue': issue,
                    'action': 'REMOVED_DUPLICATES',
                    'rows_removed': removed,
                    'status': 'SUCCESS'
                }
            
            elif issue['type'] == 'NEGATIVE_VALUES':
                # Filter out negatives
                col = issue['column']
                if col in df_fixed.columns:
                    original_count = len(df_fixed)
                    df_fixed = df_fixed.filter(pl.col(col) >= 0)
                    removed = original_count - len(df_fixed)
                    action = {
                        'issue': issue,
                        'action': 'FILTERED_NEGATIVE_VALUES',
                        'column': col,
                        'rows_removed': removed,
                        'status': 'SUCCESS'
                    }
            
            elif issue['type'] == 'HIGH_NULL_RATE':
                # Consider removing column if > 80% null
                col = issue['column']
                if col in df_fixed.columns:
                    null_pct = df_fixed[col].null_count() / len(df_fixed) * 100
                    if null_pct > 80:
                        df_fixed = df_fixed.drop(col)
                        action = {
                            'issue': issue,
                            'action': 'DROPPED_COLUMN',
                            'column': col,
                            'reason': f'{null_pct:.1f}% null values',
                            'status': 'SUCCESS'
                        }
            
            if action:
                remediation_actions.append(action)
                self.remediation_log.append(action)
        
        logger.info(f"✅ Remediation complete: {len(remediation_actions)} actions taken")
        
        return df_fixed, remediation_actions
    
    def get_remediation_summary(self) -> Dict:
        """Get summary of all remediation actions"""
        return {
            'total_actions': len(self.remediation_log),
            'action_types': pd.Series([a['action'] for a in self.remediation_log]).value_counts().to_dict(),
            'recent_actions': self.remediation_log[-10:]
        }


if __name__ == "__main__":
    # Example usage
    df = pl.DataFrame({
        'id': [1, 2, 2, 3, 4, 5],
        'name': ['Alice', '  Bob  ', 'Charlie', None, 'Dave', 'Eve'],
        'age': [25, 30, 30, -5, 40, 200],
        'salary': [50000, 60000, 60000, 70000, None, 80000],
    })
    
    # Profile
    profiler = DataProfilerAgent()
    profile = profiler.profile_dataset(df, 'test_data')
    print(json.dumps(profile, indent=2, default=str))
    
    # Quality report
    quality_agent = QualityAgent()
    report = quality_agent.generate_quality_report(df, 'test_data')
    print(f"\n📊 Quality Score: {report.quality_score}/100")
    
    # Auto-remediate
    remediation_agent = RemediationAgent()
    df_fixed, actions = remediation_agent.auto_remediate(df, profile['issues_detected'])
    print(f"\n🔧 Remediation Actions: {len(actions)}")
    print(df_fixed)