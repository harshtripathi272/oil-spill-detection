from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from typing import List, Dict, Any
from datetime import datetime, timedelta
from app.database import supabase
from app.models.incident import Incident, DagRun, Metric
from app.models.predictions import Prediction
from app.schemas.dashboard import DashboardStats, ChartData, TimeSeriesData

class DashboardService:
    def __init__(self, db: Session):
        self.db = db

    def _load_supabase_incidents(self, require_created_at: bool = False):
        incidents = supabase.list_incidents(limit=10000)
        if require_created_at:
            return [i for i in incidents if i.get('created_at')]
        return incidents

    def _average_by_date(self, incidents: List[Dict[str, Any]], value_key: str):
        totals: Dict[str, float] = {}
        counts: Dict[str, int] = {}
        for incident in incidents:
            value = incident.get(value_key)
            if value is None:
                continue
            date_key = incident.get('created_at')[:10]
            totals[date_key] = totals.get(date_key, 0.0) + value
            counts[date_key] = counts.get(date_key, 0) + 1
        dates = sorted(totals.keys())
        values = [round(totals[d] / counts[d], 2) for d in dates]
        return dates, values

    def get_dashboard_stats(self) -> DashboardStats:
        """Get overall dashboard statistics"""
        if supabase.is_configured:
            incidents = self._load_supabase_incidents()
            total_incidents = len(incidents)
            active_incidents = len([i for i in incidents if i['status'] in ['detected', 'confirmed']])
            resolved_incidents = len([i for i in incidents if i['status'] in ['resolved']])
            false_positives = len([i for i in incidents if i['status'] in ['false_positive']])
            confidence_values = [i['confidence_score'] for i in incidents if i.get('confidence_score') is not None]
            avg_confidence_score = round(sum(confidence_values) / len(confidence_values), 2) if confidence_values else 0.0
            processing_times = [i['processing_time'] for i in incidents if i.get('processing_time') is not None]
            avg_processing_time = round(sum(processing_times) / len(processing_times), 2) if processing_times else 0.0
        else:
            # Incident stats
            total_incidents = self.db.query(func.count(Incident.id)).scalar()
            active_incidents = self.db.query(func.count(Incident.id)).filter(
                Incident.status.in_(["detected", "confirmed"])
            ).scalar()
            resolved_incidents = self.db.query(func.count(Incident.id)).filter(
                Incident.status == "resolved"
            ).scalar()
            false_positives = self.db.query(func.count(Incident.id)).filter(
                Incident.status == "false_positive"
            ).scalar()

            # Processing time stats
            avg_processing_time = self.db.query(func.avg(Incident.processing_time)).filter(
                Incident.processing_time.isnot(None)
            ).scalar() or 0.0
            avg_confidence_score = self.db.query(func.avg(Incident.confidence_score)).filter(
                Incident.confidence_score.isnot(None)
            ).scalar() or 0.0

        # DAG run stats
        total_dag_runs = self.db.query(func.count(DagRun.id)).scalar()
        successful_runs = self.db.query(func.count(DagRun.id)).filter(
            DagRun.state == "success"
        ).scalar()
        failed_runs = self.db.query(func.count(DagRun.id)).filter(
            DagRun.state == "failed"
        ).scalar()

        # Prediction stats
        total_predictions = self.db.query(func.count(Prediction.id)).scalar()
        oil_spill_predictions = self.db.query(func.count(Prediction.id)).filter(
            Prediction.prediction == "oil_spill"
        ).scalar()
        no_oil_spill_predictions = self.db.query(func.count(Prediction.id)).filter(
            Prediction.prediction == "no_oil_spill"
        ).scalar()
        avg_prediction_confidence = self.db.query(func.avg(Prediction.confidence)).filter(
            Prediction.confidence.isnot(None)
        ).scalar() or 0.0

        # Recent predictions (last 24 hours)
        yesterday = datetime.utcnow() - timedelta(days=1)
        recent_predictions_24h = self.db.query(func.count(Prediction.id)).filter(
            Prediction.created_at >= yesterday
        ).scalar()

        return DashboardStats(
            total_incidents=total_incidents,
            active_incidents=active_incidents,
            resolved_incidents=resolved_incidents,
            false_positives=false_positives,
            avg_processing_time=round(avg_processing_time, 2),
            total_dag_runs=total_dag_runs,
            successful_runs=successful_runs,
            failed_runs=failed_runs,
            avg_confidence_score=round(avg_confidence_score, 2),
            total_predictions=total_predictions,
            oil_spill_predictions=oil_spill_predictions,
            no_oil_spill_predictions=no_oil_spill_predictions,
            avg_prediction_confidence=round(avg_prediction_confidence, 3),
            recent_predictions_24h=recent_predictions_24h
        )

    def get_recent_incidents(self, limit: int = 10) -> List[Incident]:
        """Get recent incidents"""
        if supabase.is_configured:
            incidents = self._load_supabase_incidents(require_created_at=True)
            sorted_incidents = sorted(
                incidents,
                key=lambda x: x.get('created_at'),
                reverse=True
            )
            return [Incident(**incident) for incident in sorted_incidents[:limit]]

        return self.db.query(Incident).order_by(
            desc(Incident.detection_time)
        ).limit(limit).all()

    def get_incidents_over_time(self, days: int = 30) -> ChartData:
        """Get incidents count over time"""
        start_date = datetime.utcnow() - timedelta(days=days)

        if supabase.is_configured:
            incidents = self._load_supabase_incidents(require_created_at=True)
            counts_by_date: Dict[str, int] = {}
            for incident in incidents:
                date_key = incident.get('created_at')[:10]
                counts_by_date[date_key] = counts_by_date.get(date_key, 0) + 1
            dates = sorted(counts_by_date.keys())
            counts = [counts_by_date[d] for d in dates]
        else:
            results = self.db.query(
                func.date(Incident.detection_time).label('date'),
                func.count(Incident.id).label('count')
            ).filter(
                Incident.detection_time >= start_date
            ).group_by(
                func.date(Incident.detection_time)
            ).order_by(
                func.date(Incident.detection_time)
            ).all()
            dates = [r.date for r in results]
            counts = [r.count for r in results]

        return ChartData(
            title="Incidents Over Time",
            data={
                "dates": dates,
                "counts": counts
            },
            chart_type="line"
        )

    def get_processing_times_chart(self, days: int = 30) -> ChartData:
        """Get processing times over time"""
        start_date = datetime.utcnow() - timedelta(days=days)

        if supabase.is_configured:
            incidents = self._load_supabase_incidents(require_created_at=True)
            dates, times = self._average_by_date(incidents, 'processing_time')
        else:
            results = self.db.query(
                func.date(Incident.detection_time).label('date'),
                func.avg(Incident.processing_time).label('avg_time')
            ).filter(
                Incident.detection_time >= start_date,
                Incident.processing_time.isnot(None)
            ).group_by(
                func.date(Incident.detection_time)
            ).order_by(
                func.date(Incident.detection_time)
            ).all()
            dates = [r.date for r in results]
            times = [round(r.avg_time, 2) for r in results]

        return ChartData(
            title="Average Processing Time",
            data={
                "dates": dates,
                "processing_times": times
            },
            chart_type="line"
        )

    def get_status_distribution(self) -> ChartData:
        """Get incident status distribution"""
        if supabase.is_configured:
            incidents = self._load_supabase_incidents()
            distribution = {}
            for incident in incidents:
                status = incident.get('status')
                if status is None:
                    continue
                distribution[status] = distribution.get(status, 0) + 1
            statuses = list(distribution.keys())
            counts = list(distribution.values())
        else:
            results = self.db.query(
                Incident.status,
                func.count(Incident.id).label('count')
            ).group_by(Incident.status).all()
            statuses = [r.status for r in results]
            counts = [r.count for r in results]

        return ChartData(
            title="Incident Status Distribution",
            data={
                "statuses": statuses,
                "counts": counts
            },
            chart_type="pie"
        )

    def get_model_performance(self, days: int = 30) -> ChartData:
        """Get model performance metrics over time"""
        start_date = datetime.utcnow() - timedelta(days=days)

        if supabase.is_configured:
            incidents = self._load_supabase_incidents(require_created_at=True)
            dates, confidences = self._average_by_date(incidents, 'confidence_score')
        else:
            results = self.db.query(
                func.date(Incident.detection_time).label('date'),
                func.avg(Incident.confidence_score).label('avg_confidence')
            ).filter(
                Incident.detection_time >= start_date,
                Incident.confidence_score.isnot(None)
            ).group_by(
                func.date(Incident.detection_time)
            ).order_by(
                func.date(Incident.detection_time)
            ).all()
            dates = [r.date for r in results]
            confidences = [round(r.avg_confidence, 2) for r in results]

        return ChartData(
            title="Model Confidence Over Time",
            data={
                "dates": dates,
                "confidence_scores": confidences
            },
            chart_type="line"
        )

    def get_dag_run_performance(self, days: int = 7) -> ChartData:
        """Get DAG run performance metrics"""
        start_date = datetime.utcnow() - timedelta(days=days)

        results = self.db.query(
            DagRun.state,
            func.count(DagRun.id).label('count')
        ).filter(
            DagRun.start_date >= start_date
        ).group_by(DagRun.state).all()

        states = [r.state for r in results]
        counts = [r.count for r in results]

        return ChartData(
            title="DAG Run Status Distribution",
            data={
                "states": states,
                "counts": counts
            },
            chart_type="bar"
        )