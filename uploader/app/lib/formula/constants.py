from dataclasses import dataclass
from typing import final

import astropy.constants as const
import astropy.units as u
import numpy as np


@final
@dataclass(frozen=True)
class ConstantDef:
    name: str
    value: u.Quantity
    detail: str

    @property
    def insert(self) -> str:
        return self.name


NAMED_CONSTANTS: tuple[ConstantDef, ...] = (
    ConstantDef("pi", np.pi * u.dimensionless_unscaled, "Pi"),
    ConstantDef("c", const.c, "Speed of light"),
    ConstantDef("G", const.G, "Gravitational constant"),
    ConstantDef("h", const.h, "Planck constant"),
    ConstantDef("k_B", const.k_B, "Boltzmann constant"),
    ConstantDef("sigma", const.sigma_sb, "Stefan-Boltzmann constant"),
    ConstantDef("m_e", const.m_e, "Electron mass"),
    ConstantDef("m_p", const.m_p, "Proton mass"),
    ConstantDef("au", 1 * u.au, "Astronomical unit"),
    ConstantDef("pc", 1 * u.pc, "Parsec"),
    ConstantDef("ly", 1 * u.lyr, "Light year"),
    ConstantDef("eV", 1 * u.eV, "Electronvolt"),
    ConstantDef("Jy", 1 * u.Jy, "Jansky"),
    ConstantDef("M_sun", const.M_sun, "Solar mass"),
    ConstantDef("R_sun", const.R_sun, "Nominal solar radius"),
    ConstantDef("L_sun", const.L_sun, "Nominal solar luminosity"),
    ConstantDef("M_earth", const.M_earth, "Earth mass"),
    ConstantDef("M_jup", const.M_jup, "Jupiter mass"),
    ConstantDef("deg", 1 * u.deg, "Degree"),
    ConstantDef("rad", 1 * u.rad, "Radian"),
    ConstantDef("arcmin", 1 * u.arcmin, "Arcminute"),
    ConstantDef("arcsec", 1 * u.arcsec, "Arcsecond"),
    ConstantDef("mag", 1 * u.mag, "Magnitude"),
)
