"""
Data models for Law Firm Matcher.
SQLite via SQLAlchemy.
"""
from datetime import datetime
from sqlalchemy import (
    create_engine, Column, Integer, String, Text, Float, Boolean,
    DateTime, ForeignKey, UniqueConstraint, Index
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

Base = declarative_base()


class IndexedFirm(Base):
    """Master cross-tenant law firm record (the canonical indexed name)."""
    __tablename__ = "indexed_firms"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, unique=True)
    is_active = Column(Boolean, default=True, nullable=False)
    notes = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    monikers = relationship("Moniker", back_populates="indexed_firm", cascade="all, delete-orphan")
    ma_acquisitions = relationship("MaRule", foreign_keys="MaRule.acquiring_firm_id", back_populates="acquiring_firm")

    def __repr__(self):
        return f"<IndexedFirm(id={self.id}, name='{self.name}')>"


class Moniker(Base):
    """Alternative names for an indexed firm (as seen in different servicer systems)."""
    __tablename__ = "monikers"

    id = Column(Integer, primary_key=True)
    indexed_firm_id = Column(Integer, ForeignKey("indexed_firms.id"), nullable=False)
    name = Column(String(255), nullable=False)
    source = Column(String(100), default="")  # e.g., "ServiceMac", "Valon production", "LoanCare"
    notes = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow)

    indexed_firm = relationship("IndexedFirm", back_populates="monikers")

    __table_args__ = (
        UniqueConstraint("indexed_firm_id", "name", name="uq_moniker_firm_name"),
        Index("ix_moniker_name", "name"),
    )

    def __repr__(self):
        return f"<Moniker('{self.name}' -> '{self.indexed_firm.name}')>"


class MaRule(Base):
    """M&A context rule: tracks when one firm acquired/merged with another.

    These rules are applied during dedup to automatically group related firms.
    Example: BWW Law was acquired by Aldridge Pite -> context = "acq. BWW"
    """
    __tablename__ = "ma_rules"

    id = Column(Integer, primary_key=True)
    acquired_name = Column(String(255), nullable=False)  # the old/acquired firm name
    acquiring_firm_id = Column(Integer, ForeignKey("indexed_firms.id"), nullable=False)
    context_label = Column(String(255), nullable=False)  # e.g., "acq. BWW", "fka Hutchens / Wilson & Associates"
    notes = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow)

    acquiring_firm = relationship("IndexedFirm", back_populates="ma_acquisitions")

    __table_args__ = (
        Index("ix_ma_acquired", "acquired_name"),
    )

    def __repr__(self):
        return f"<MaRule('{self.acquired_name}' -> '{self.acquiring_firm.name}', context='{self.context_label}')>"


class FirmTracker(Base):
    """Persistent onboarding tracker for each indexed firm.

    This is the single source of truth for where each firm stands
    in the onboarding process — replaces the 'firm tracker' spreadsheet.
    """
    __tablename__ = "firm_tracker"

    id = Column(Integer, primary_key=True)
    indexed_firm_id = Column(Integer, ForeignKey("indexed_firms.id"), nullable=False, unique=True)

    # Servicer presence
    vm_firm = Column(Boolean, default=False)
    vm_active_fcl = Column(String(50), default="")    # VM active foreclosure case count
    vm_active_bk = Column(String(50), default="")     # VM active bankruptcy case count
    nrz_rank = Column(String(50), default="")       # e.g., "3", "Not in top 10"
    loancare_rank = Column(String(50), default="")   # e.g., "1", "Not in top 90%"

    # Ocean program
    ocean_design_partner = Column(Boolean, default=False)
    ocean_m1 = Column(Boolean, default=False)
    ocean_m2 = Column(String(20), default="No")      # Yes / No / Fell off
    ocean_m2_volume = Column(String(50), default="")  # M2 volume count

    # Onboarding
    proposed_wave = Column(String(100), default="")   # e.g., "Pilot", "Wave 1 (Feb 26)"
    live_training = Column(Boolean, default=False)
    wave_notes = Column(Text, default="")

    # Engagement
    last_reachout = Column(String(50), default="")
    phase0_meeting = Column(Boolean, default=False)
    leadership_meeting = Column(Boolean, default=False)
    design_meeting = Column(Boolean, default=False)
    leadership_engagement = Column(String(20), default="")  # Yes / No / blank
    interaction = Column(String(50), default="")      # e.g., "Design partner", "Intro call", "Planned engagement"

    # Contacts
    ops_contact_email = Column(String(255), default="")
    leadership_contact = Column(String(255), default="")
    leadership_title = Column(String(255), default="")
    leadership_email = Column(String(255), default="")
    nda_executed_by = Column(String(255), default="")

    # Notes
    notes = Column(Text, default="")

    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    indexed_firm = relationship("IndexedFirm", backref="tracker")

    def __repr__(self):
        return f"<FirmTracker(firm='{self.indexed_firm.name}', wave='{self.proposed_wave}')>"


class ServicerList(Base):
    """An uploaded firm list from a servicer (e.g., ServiceMac M2 list)."""
    __tablename__ = "servicer_lists"

    id = Column(Integer, primary_key=True)
    servicer_name = Column(String(100), nullable=False)
    milestone = Column(String(50), default="")  # e.g., "M2", "M3"
    filename = Column(String(255), default="")
    uploaded_at = Column(DateTime, default=datetime.utcnow)
    notes = Column(Text, default="")

    entries = relationship("ServicerListEntry", back_populates="servicer_list", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<ServicerList('{self.servicer_name} {self.milestone}', {len(self.entries)} entries)>"


class ServicerListEntry(Base):
    """Individual firm entry from an uploaded servicer list."""
    __tablename__ = "servicer_list_entries"

    id = Column(Integer, primary_key=True)
    servicer_list_id = Column(Integer, ForeignKey("servicer_lists.id"), nullable=False)
    raw_name = Column(String(255), nullable=False)
    matched_firm_id = Column(Integer, ForeignKey("indexed_firms.id"), nullable=True)
    match_score = Column(Float, nullable=True)
    match_status = Column(String(50), default="pending")  # pending, auto_matched, confirmed, rejected, new
    wave = Column(String(50), default="")  # Pilot, Wave 1, Wave 2, etc.
    notes = Column(Text, default="")

    servicer_list = relationship("ServicerList", back_populates="entries")
    matched_firm = relationship("IndexedFirm")

    __table_args__ = (
        Index("ix_entry_raw_name", "raw_name"),
    )


class ValonosEntity(Base):
    """A law firm entity as it exists in ValonOS production (servicer_law_firms table)."""
    __tablename__ = "valonos_entities"

    id = Column(Integer, primary_key=True)
    sid = Column(String(50), nullable=False, unique=True)  # ValonOS SID
    name = Column(String(255), nullable=False)
    tenant_key = Column(Integer, nullable=False)
    tenant_name = Column(String(100), default="")
    is_active = Column(Boolean, default=True)
    indexed_firm_id = Column(Integer, ForeignKey("indexed_firms.id"), nullable=True)
    synced_at = Column(DateTime, default=datetime.utcnow)

    indexed_firm = relationship("IndexedFirm", backref="valonos_entities")

    __table_args__ = (
        Index("ix_valonos_entity_name", "name"),
    )

    def __repr__(self):
        return f"<ValonosEntity('{self.name}', tenant={self.tenant_name}, active={self.is_active})>"


def get_engine(db_path="data/law_firms.db"):
    return create_engine(f"sqlite:///{db_path}", echo=False)


def get_session(db_path="data/law_firms.db"):
    engine = get_engine(db_path)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()
